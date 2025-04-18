package common

import (
	"bufio"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	"movies-analysis/client/common/communication"
	"movies-analysis/client/config"
)

var log = config.Log

const (
	MAX_BATCH_SIZE       = 8 * 1024
	CODE_AGENCY          = 'A'
	CODE_BATCH           = 'B'
	CODE_WAIT_FOR_RESULT = 'W'
	CODE_RESULT          = 'R'
	CODE_END             = 'E'
	CODE_WINNER          = 'S'
	SIZE_HEADER          = 4
	SIZE_CODE            = 1
)

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID             string
	ServerAddress  string
	LoopAmount     int
	LoopPeriod     time.Duration
	MaxAmount      int
	Phase          int
	FilesToProcess []string // Lista de archivos a procesar
}

// Struct for the bet
type Bet struct {
	Id         int
	Nombre     string
	Apellido   string
	Documento  int
	Nacimiento string
	Numero     int
}

func (b *Bet) serialize() []byte {
	return []byte(fmt.Sprintf(
		"%d;%s;%s;%d;%s;%d",
		b.Id,
		b.Nombre,
		b.Apellido,
		b.Documento,
		b.Nacimiento,
		b.Numero,
	))
}

// Client Entity that encapsulates how
type Client struct {
	config ClientConfig
	socket *communication.Socket
	quit   chan os.Signal
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig) *Client {
	socket, err := communication.NewSocket(config.ServerAddress)
	if err != nil {
		log.Criticalf("action: create_socket | result: fail | error: %v", err)
		return nil
	}

	client := &Client{
		config: config,
		socket: socket,
		quit:   make(chan os.Signal, 1),
	}
	signal.Notify(client.quit, syscall.SIGINT, syscall.SIGTERM)
	return client
}

func (c *Client) StartClientLoop() {
	defer c.closeClient()

	err := c.socket.SendAll([]byte{0}, SIZE_CODE)
	if err != nil {
		log.Errorf("action: send_message_code_batch | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
		// return true
	}
	// Procesar cada archivo de la lista
	// for _, filename := range c.config.FilesToProcess {
	// if err := c.processFile("movies.csv"); err != nil {
	// 	log.Errorf("action: process_file | file: %s | result: fail | error: %v", "movies.csv", err)
	// 	// continue
	// }
	// }

	// Enviar mensaje de finalización
	c.config.Phase = CODE_END
	c.handleCloseConnection()
}

func (c *Client) processFile(filename string) error {
	log.Infof("action: process_file | file: %s | result: start", filename)

	// Abrir el archivo
	file, err := os.Open(filename)
	if err != nil {
		return fmt.Errorf("failed to open file: %v", err)
	}
	defer file.Close()

	fileReader := bufio.NewReader(file)

	// Procesar el archivo hasta que se complete o haya una señal de salida
	for {
		select {
		case <-c.quit:
			return fmt.Errorf("client received quit signal")
		default:
			finishedProcessing := c.handlePhase(fileReader)
			if finishedProcessing {
				log.Infof("action: process_file | file: %s | result: complete", filename)
				return nil
			}
			// Esperar antes de continuar con el siguiente batch
			time.Sleep(c.config.LoopPeriod)
		}
	}
}

func (c *Client) handlePhase(reader *bufio.Reader) bool {
	switch c.config.Phase {
	case CODE_BATCH:
		return c.handleBatch(reader)
	case CODE_END:
		c.handleCloseConnection()
		return true
	default:
		log.Criticalf("action: handle_phase | result: fail | client_id: %v | error: invalid phase",
			c.config.ID,
		)
	}
	return false
}

func (c *Client) handleBatch(reader *bufio.Reader) bool {
	batch, eof := c.createBatch(reader)
	if eof {
		// c.config.Phase = CODE_END
		return true
	}

	err := c.socket.SendAll([]byte{CODE_BATCH}, SIZE_CODE)
	if err != nil {
		log.Errorf("action: send_message_code_batch | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
		return true
	}

	errBatch := c.sendBatch(batch)
	if errBatch != nil {
		log.Errorf("action: send_message_batch | result: fail | client_id: %v",
			c.config.ID,
		)
		return true
	}

	response, errResponse := c.recvResponse()
	if errResponse != nil {
		log.Errorf("action: receive_status_batch | result: fail | client_id: %v | error: %v",
			c.config.ID,
			errResponse,
		)
		return true
	}

	c.parseResponse(response)
	return false
}

func (c *Client) createBatch(reader *bufio.Reader) ([]byte, bool) {
	var batchData []byte
	betCount := 0
	eof := false

	for betCount < c.config.MaxAmount {
		bet, err := c.readLine(reader)
		if err != nil {
			log.Criticalf("action: read_bet | result: fail | error: %v", err)
			return nil, true
		}
		if bet == nil {
			eof = true
			break
		}

		serializeBet := bet

		if len(batchData) > 0 && len(batchData)+len(serializeBet)+1 > MAX_BATCH_SIZE {
			break
		}

		if len(batchData) > 0 {
			batchData = append(batchData, '|')
		}

		batchData = append(batchData, serializeBet...)
		betCount++
	}

	return batchData, eof
}

func (c *Client) sendBatch(data []byte) error {
	totalBytes := len(data)

	header := fmt.Sprintf("%04d", totalBytes)

	errHeader := c.socket.SendAll([]byte(header), SIZE_HEADER)
	if errHeader != nil {
		log.Errorf("action: send_message_header | result: fail | client_id: %v | error: Invalid Header: %v",
			c.config.ID,
			errHeader,
		)
		return errHeader
	}

	errData := c.socket.SendAll(data, totalBytes)
	if errData != nil {
		log.Errorf("action: send_message_data | result: fail | client_id: %v | error: Invalid data: %v",
			c.config.ID,
			errData,
		)
		return errData
	}
	log.Infof("action: send_batch | result: success | client_id: %v | total_send: %d", c.config.ID, totalBytes)
	return nil
}

func (c *Client) recvResponse() ([]byte, error) {
	respHeader, errHeader := c.socket.ReceiveAll(SIZE_HEADER)
	if errHeader != nil {
		log.Criticalf("action: read_response_header | result: fail")
		return nil, errHeader
	}

	respSize, err := strconv.Atoi(string(respHeader))
	if err != nil {
		log.Criticalf("action: parse_response_header | result: fail | error: %v", err)
		return nil, err
	}

	respData, errResp := c.socket.ReceiveAll(respSize)
	if errResp != nil {
		log.Criticalf("action: read_response | result: fail | error: %v", err)
		return nil, errResp
	}

	return respData, nil
}

func (c *Client) readLine(reader *bufio.Reader) ([]byte, error) {
	line, err := reader.ReadString('\n')
	if err != nil {
		if err.Error() == "EOF" {
			return nil, nil
		}
		log.Criticalf("action: read_line | result: fail | error: %v", err)
		return nil, err
	}
	return []byte(line), nil
}

func (c *Client) handleCloseConnection() {
	err := c.socket.SendAll([]byte{CODE_END}, SIZE_CODE)
	if err != nil {
		log.Criticalf("action: send_message_end | result: fail")
	}
	log.Infof("action: close_connection | result: success")
}

func (c *Client) parseResponse(response []byte) {
	responseParts := strings.Split(string(response), ";")
	if len(responseParts) != 2 {
		log.Criticalf("action: parse_response | result: fail | client_id: %v | error: invalid response format",
			c.config.ID,
		)
		return
	}

	code := responseParts[0]

	lenght, err := strconv.Atoi(responseParts[1])
	if err != nil {
		log.Criticalf("action: parse_response | result: fail | error: %v", err)
		return
	}

	switch code {
	case "FAIL":
		log.Errorf("action: apuesta_recibida | result: fail | cantidad: %d", lenght)
		return
	default:
		return
	}
}

func (c *Client) closeClient() {
	time.Sleep(5 * time.Second)
	if c.socket.IsConnected() {
		log.Infof("action: exit | result: success | client_id: %v", c.config.ID)
		c.socket.Close()
	}
}
