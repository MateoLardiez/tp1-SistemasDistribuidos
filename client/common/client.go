package common

import (
	"fmt"
	"io"
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

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID             string
	ServerAddress  string
	LoopAmount     int
	LoopPeriod     time.Duration
	MaxAmount      int
	Phase          int
	FilesToProcess []string // Lista de archivos a procesar
	Query          int
}

// Client Entity that encapsulates how1
type Client struct {
	config   ClientConfig
	protocol *communication.Protocol
	quit     chan os.Signal
}

// NewClient Initializes a new client receiving the configuration
// as a parameter
func NewClient(config ClientConfig) *Client {
	protocol, err := communication.NewProtocol(config.ServerAddress)
	if err != nil {
		log.Criticalf("action: create_protocol | result: fail | error: %v", err)
		return nil
	}

	client := &Client{
		config:   config,
		protocol: protocol,
		quit:     make(chan os.Signal, 1),
	}
	signal.Notify(client.quit, syscall.SIGINT, syscall.SIGTERM)
	return client
}

func (c *Client) StartClientLoop() {
	defer c.closeClient()

	for {
		select {
		case <-c.quit:
			// log.Infof("action: client_loop | result: quit")
			// c.handleCloseConnection()
			c.closeClient()
			return
		default:
			// c.handleClientLoop()
			finishedProcessing := c.handlePhase()
			if finishedProcessing {
				log.Infof("action: client_loop | result: complete")
				return
			}
		}
	}

	// c.config.Phase = CODE_END
	// c.handleCloseConnection()
	// Procesar cada archivo de la lista
	// for _, filename := range c.config.FilesToProcess {
	// 	if err := c.processFile(filename); err != nil {
	// 		log.Errorf("action: process_file | file: %s | result: fail | error: %v", filename, err)
	// 		continue
	// 	}
	// }

	// Enviar mensaje de finalización
}

func (c *Client) handlePhase() bool {
	switch c.config.Phase {
	case communication.CODE_QUERY:
		c.handleQuery()
	case communication.CODE_END:
		c.handleCloseConnection()
		return true
	default:
		log.Criticalf("action: handle_phase | result: fail | client_id: %v | error: invalid phase",
			c.config.ID,
		)
	}
	return false
}

func (c *Client) handleQuery() {
	switch c.config.Query {
	case communication.ALL_QUERYS:
		c.handleAllQueries()
	// case QUERY_1:
	// 	c.handleQuery1()
	// case QUERY_2:
	// 	c.handleQuery2()
	// case QUERY_3:
	// 	c.handleQuery3()
	// case QUERY_4:
	// 	c.handleQuery4()
	// case QUERY_5:
	// 	c.handleQuery5()
	default:
		log.Criticalf("action: handle_query | result: fail | client_id: %v | error: invalid query",
			c.config.ID,
		)
	}
}

func (c *Client) handleAllQueries() {
	err := c.protocol.SendCodeQuery(communication.ALL_QUERYS)
	if err != nil {
		log.Errorf("action: send_message_code_query | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
		return
	}
	c.SendMoviesFile()
	// c.SendRatingsFile()
	// c.SendCreditsFile()

	// c.processFile()
	c.config.Phase = communication.CODE_END
}

func (c *Client) SendMoviesFile() {
	reader, err := NewFileReader("movies.csv")
	if err != nil {
		log.Errorf("action: open_file | result: fail | error: %v", err)
		return
	}
	defer reader.Close()

	for {
		finishedSending := c.handleBatch(reader)
		if finishedSending {
			log.Infof("action: send_movies_file | result: complete")
			break
		}
	}
}

func (c *Client) createBatch(reader *FileReader) ([]byte, bool) {
	var batch []byte
	lineCount := 0
	eof := false

	for lineCount < c.config.MaxAmount && len(batch) < communication.MAX_BATCH_SIZE {
		line, err := reader.ReadLine()
		// Manejar explícitamente el EOF
		if err == io.EOF {
			eof = true
			// Si ya tenemos contenido para enviar, lo hacemos
			if len(batch) > 0 {
				break
			}
			// Si no hay contenido y es EOF, indicamos que hemos terminado
			return nil, eof
		} else if err != nil {
			return nil, true
		}

		// Convertir la línea a bytes
		serializedLine := []byte(line)

		// Para la primera línea no añadimos separador
		if lineCount > 0 || len(batch) > 0 {
			// Verificar si añadir el separador superaría el tamaño máximo
			if len(batch)+1+len(serializedLine) > communication.MAX_BATCH_SIZE {
				break
			}
			batch = append(batch, '|')
		}

		// Añadir la línea al batch
		batch = append(batch, serializedLine...)
		lineCount++
	}

	log.Infof("action: create_batch | result: success | client_id: %v | total_lines: %d", c.config.ID, lineCount)

	// Añadir el carácter de nueva línea al final si cabe
	if len(batch)+1 <= communication.MAX_BATCH_SIZE {
		batch = append(batch, '\n')
	}

	return batch, eof
}

func (c *Client) handleBatch(reader *FileReader) bool {
	batch, eof := c.createBatch(reader)
	if eof {
		// c.config.Phase = CODE_END
		log.Infof("action: handle_batch_finish | result: complete | client_id: %v", c.config.ID)
		return true
	}

	err := c.protocol.SendAll([]byte{communication.CODE_BATCH}, communication.SIZE_CODE)
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

func (c *Client) sendBatch(data []byte) error {
	totalBytes := len(data)

	header := fmt.Sprintf("%04d", totalBytes)

	errHeader := c.protocol.SendAll([]byte(header), communication.SIZE_HEADER)
	if errHeader != nil {
		log.Errorf("action: send_message_header | result: fail | client_id: %v | error: Invalid Header: %v",
			c.config.ID,
			errHeader,
		)
		return errHeader
	}

	errData := c.protocol.SendAll(data, totalBytes)
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
	respHeader, errHeader := c.protocol.ReceiveAll(communication.SIZE_HEADER)
	if errHeader != nil {
		log.Criticalf("action: read_response_header | result: fail")
		return nil, errHeader
	}

	respSize, err := strconv.Atoi(string(respHeader))
	if err != nil {
		log.Criticalf("action: parse_response_header | result: fail | error: %v", err)
		return nil, err
	}

	respData, errResp := c.protocol.ReceiveAll(respSize)
	if errResp != nil {
		log.Criticalf("action: read_response | result: fail | error: %v", err)
		return nil, errResp
	}

	return respData, nil
}

func (c *Client) handleCloseConnection() {
	err := c.protocol.SendAll([]byte{communication.CODE_END}, communication.SIZE_CODE)
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
	if err := c.protocol.Close(); err != nil {
		log.Errorf("action: close_protocol | result: fail | error: %v", err)
		return
	}
}
