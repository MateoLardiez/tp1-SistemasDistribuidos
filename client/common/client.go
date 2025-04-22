package common

import (
	"io"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"movies-analysis/client/common/communication"
	"movies-analysis/client/config"
)

var log = config.Log

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID             int
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
	config          ClientConfig
	protocol        *communication.Protocol
	quit            chan os.Signal
	bufferedLine    string // Buffer to store a line that didn't fit in the previous batch
	hasBufferedLine bool   // Flag to indicate if we have a buffered line
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
		config:          config,
		protocol:        protocol,
		quit:            make(chan os.Signal, 1),
		hasBufferedLine: false,
	}
	signal.Notify(client.quit, syscall.SIGINT, syscall.SIGTERM)
	return client
}

func (c *Client) StartClientLoop() {
	defer c.closeClient()

	for {
		select {
		case <-c.quit:
			c.closeClient()
			return
		default:
			finishedProcessing := c.handlePhase()
			if finishedProcessing {
				log.Infof("action: client_loop | result: complete")
				return
			}
		}
	}
}

func (c *Client) handlePhase() bool {
	switch c.config.Phase {
	case communication.CODE_QUERY:
		return c.handleQuery()
	case communication.CODE_END:
		c.handleCloseConnection()
		return true
	default:
		log.Criticalf("action: handle_phase | result: fail | client_id: %v | error: invalid phase",
			c.config.ID,
		)
		return true
	}
	// return false
}

func (c *Client) handleQuery() bool {
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
		return true
	}
	return false
}

func (c *Client) handleAllQueries() {
	message := communication.NewMessageProtocol(
		c.config.ID,
		communication.TYPE_QUERY,
		[]byte(strconv.Itoa(communication.ALL_QUERYS)),
	)
	err := c.protocol.SendMessage(message)
	if err != nil {
		log.Errorf("action: send_message_code_query | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
		return
	}
	// c.SendFile("movies.csv", communication.BATCH_MOVIES, communication.EOF_MOVIES)
	// c.SendFile("ratings.csv", communication.BATCH_RATINGS, communication.EOF_RATINGS)
	c.SendFile("credits.csv", communication.BATCH_CREDITS, communication.EOF_CREDITS)
	messageFinish := communication.NewMessageProtocol(
		c.config.ID,
		communication.FINISH_SEND_FILES,
		nil,
	)
	errFinish := c.protocol.SendMessage(messageFinish)
	if errFinish != nil {
		log.Errorf("action: send_message_code_end | result: fail | client_id: %v | error: %v",
			c.config.ID,
			errFinish,
		)
		return
	}
	c.config.Phase = communication.CODE_END
}

func (c *Client) SendFile(filename string, code int, codeEOF int) {
	reader, err := NewFileReader(filename)
	if err != nil {
		log.Errorf("action: open_file | result: fail | error: %v", err)
		return
	}
	defer reader.Close()

	for {
		finishedSending := c.handleBatch(reader, code)
		if finishedSending {
			log.Infof("action: send_movies_file | result: complete")
			break
		}
	}
	messageEOF := communication.NewMessageProtocol(
		c.config.ID,
		codeEOF,
		nil,
	)

	errEnd := c.protocol.SendMessage(messageEOF)
	if errEnd != nil {
		log.Errorf("action: send_message_code_end | result: fail | client_id: %v | error: %v",
			c.config.ID,
			errEnd,
		)
		return
	}
}

func (c *Client) createBatch(reader *FileReader) ([]byte, bool) {
	var batch []byte
	lineCount := 0
	eof := false

	// First, add any buffered line from previous batch if it exists
	if c.hasBufferedLine {
		serializedLine := []byte(c.bufferedLine)
		batch = append(batch, serializedLine...)
		c.hasBufferedLine = false
		c.bufferedLine = ""
		lineCount++
	}

	for lineCount < c.config.MaxAmount {
		line, err := reader.ReadLine()
		if err == io.EOF {
			eof = true
			break
		} else if err != nil {
			return nil, true
		}
		serializedLine := []byte(line)

		if len(batch) > 0 && len(serializedLine)+len(batch)+1 > communication.MAX_BATCH_SIZE {
			c.bufferedLine = line
			c.hasBufferedLine = true
			break
		}

		if len(batch) > 0 {
			batch = append(batch, '|')
		}

		batch = append(batch, serializedLine...)
		lineCount++
	}

	log.Infof("action: create_batch | result: success | client_id: %v | total_lines: %d", c.config.ID, lineCount)

	return batch, eof
}

func (c *Client) handleBatch(reader *FileReader, code int) bool {
	eof := false
	batch, eof := c.createBatch(reader)
	if eof {
		if len(batch) == 0 {
			log.Infof("action: handle_batch | result: complete | client_id: %v", c.config.ID)
			return eof
		}
	}

	messageBatch := communication.NewMessageProtocol(
		c.config.ID,
		code,
		batch,
	)

	err := c.protocol.SendMessage(messageBatch)
	if err != nil {
		log.Errorf("action: send_message_batch | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
		return true
	}

	// Wait for the ACK message
	errResponse := c.recvResponse()
	if errResponse != nil {
		log.Errorf("action: receive_status_batch | result: fail | client_id: %v | error: %v",
			c.config.ID,
			errResponse,
		)
		return true
	}

	return eof
}

func (c *Client) recvResponse() error {
	response, err := c.protocol.ReceiveMessage()
	if err != nil {
		log.Errorf("action: receive_response_batch | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
		return err
	}
	// if response.TypeMessage == communication.TYPE_ACK {
	// 	log.Infof("action: %v | result: success | client_id: %v",
	// 		string(response.Payload),
	// 		c.config.ID,
	// 	)
	// 	return nil
	// }
	if response.TypeMessage == communication.TYPE_ERROR {
		log.Errorf("action: receive_status_batch | result: fail | client_id: %v | error: %s",
			c.config.ID,
			string(response.Payload),
		)
		return nil
	}

	return nil
}

func (c *Client) handleCloseConnection() {
	message := communication.NewMessageProtocol(
		c.config.ID,
		communication.TYPE_FINISH_COMMUNICATION,
		nil,
	)
	err := c.protocol.SendMessage(message)
	if err != nil {
		log.Criticalf("action: send_message_end | result: fail")
	}
	log.Infof("action: close_connection | result: success")
}

func (c *Client) closeClient() {
	time.Sleep(5 * time.Second)
	if err := c.protocol.Close(); err != nil {
		log.Errorf("action: close_protocol | result: fail | error: %v", err)
		return
	}
}
