package common

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"movies-analysis/client/common/communication"
	"movies-analysis/client/common/result"
	"movies-analysis/client/config"
)

var log = config.Log

// ClientConfig Configuration used by the client
type ClientConfig struct {
	ID             string
	ServerAddress  string
	TesterAddress  string // Address of the tester server
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
	bufferedLine    string                      // Buffer to store a line that didn't fit in the previous batch
	hasBufferedLine bool                        // Flag to indicate if we have a buffered line
	resultQueries   map[int]*result.ResultQuery // Store results of queries
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
		resultQueries:   make(map[int]*result.ResultQuery),
	}
	signal.Notify(client.quit, syscall.SIGINT, syscall.SIGTERM)
	return client
}

func (c *Client) StartClientLoop() {
	// defer c.closeClient()

	for {
		select {
		case <-c.quit:
			c.closeClient()
			return
		default:
			finishedProcessing := c.handlePhase()
			if finishedProcessing {
				log.Infof("action: client_loop | result: complete")
				c.closeClient()
				return
			}
		}
	}
}

func (c *Client) handlePhase() bool {
	switch c.config.Phase {
	case communication.CODE_INIT:
		return c.handleInit()
	case communication.CODE_QUERY:
		return c.handleQuery()
	case communication.CODE_RESPONSE:
		return c.handleResponse()
	case communication.CODE_END:
		c.handleCloseConnection()
		return true
	default:
		log.Criticalf("action: handle_phase | result: fail | client_id: %v | error: invalid phase",
			c.config.ID,
		)
		return true
	}
}

func (c *Client) handleInit() bool {
	log.Infof("action: handle_init | result: success | client_id: %v", c.config.ID)

	// Send the client ID to the server
	message := communication.NewMessageProtocol(
		communication.TYPE_INIT,
		[]byte{},
	)
	err := c.protocol.SendMessage(message)
	if err != nil {
		log.Errorf("action: send_message_code_init | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
		return true
	}

	recv_id, err := c.protocol.ReceiveMessage()
	if err != nil {
		log.Errorf("action: receive_message_code_init | result: fail | client_id: %v | error: %v",
			c.config.ID,
			err,
		)
		return true
	}

	if recv_id.TypeMessage != communication.CODE_INIT {
		log.Errorf("action: receive_message_code_init | result: fail | client_id: %v | error: invalid message type | type: %v",
			c.config.ID,
			recv_id.TypeMessage,
		)
		return true
	}

	c.config.ID = string(recv_id.Payload)
	log.Infof("action: receive_message_code_init | result: success | new_id: %v",
		c.config.ID,
	)

	c.config.Phase = communication.CODE_QUERY
	return false
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
	c.SendFile("movies.csv", communication.BATCH_MOVIES, communication.EOF_MOVIES, "movies")
	c.SendFile("credits.csv", communication.BATCH_CREDITS, communication.EOF_CREDITS, "credits")
	c.SendFile("ratings.csv", communication.BATCH_RATINGS, communication.EOF_RATINGS, "ratings")
	messageFinish := communication.NewMessageProtocol(
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
	c.config.Phase = communication.CODE_RESPONSE
}

func (c *Client) handleResponse() bool {
	log.Infof("action: handle_response | result: success | client_id: %v",
		c.config.ID,
	)
	result_received := 0
	finishedMessages := false

	for {
		if c.resultReceived(result_received) {
			log.Infof("action: Received_result | result: complete | client_id: %v",
				c.config.ID,
			)
			finishedMessages = false
			c.config.Phase = communication.CODE_END
			break
		}
		message, err := c.protocol.ReceiveMessage()
		if err != nil {
			log.Errorf("action: receive_message_code_response | result: fail | client_id: %v | error: %v",
				c.config.ID,
				"nil message",
			)
		}

		c.handler_message_response(message, &result_received)
	}
	return finishedMessages
}

func (c *Client) resultReceived(resultReceived int) bool {
	switch c.config.Query {
	case communication.ALL_QUERYS:
		if resultReceived == 5 {
			// Print and send results to tester
			payload := c.fmtResultQueries()
			c.sendResultsToTester(payload)
			return true
		}
	default:
		if resultReceived == 1 {
			return true
		}
	}
	return false
}

func (c *Client) fmtResultQueries() string {
	// Create a map to hold all query results
	allResults := make(map[string]interface{})

	for i := 1; i <= 5; i++ {
		if resultQuery, exists := c.resultQueries[i]; exists {
			// Convert result to JSON format
			queryKey := fmt.Sprintf("query_%d", i)
			allResults[queryKey] = resultQuery.ParseToMap()
		}
	}

	// Convert the entire result to JSON
	jsonData, err := json.MarshalIndent(allResults, "", "  ")
	if err != nil {
		log.Errorf("action: format_results_to_json | result: fail | error: %v", err)
		return "{}"
	}

	// Print the JSON result
	log.Infof("action: results | result: success | client_id: %v | json: %s",
		c.config.ID,
		string(jsonData),
	)
	return string(jsonData)
}

func (c *Client) handler_message_response(message *communication.MessageProtocol, resultReceived *int) {
	switch message.TypeMessage {
	case communication.RESULT_QUERY_1:
		c.handleQueryResult(communication.QUERY_1, message)
	case communication.RESULT_QUERY_2:
		c.handleQueryResult(communication.QUERY_2, message)
	case communication.RESULT_QUERY_3:
		c.handleQueryResult(communication.QUERY_3, message)
	case communication.RESULT_QUERY_4:
		c.handleQueryResult(communication.QUERY_4, message)
	case communication.RESULT_QUERY_5:
		c.handleQueryResult(communication.QUERY_5, message)
	case communication.EOF_QUERY_1:
		*resultReceived++
	case communication.EOF_QUERY_2:
		*resultReceived++
	case communication.EOF_QUERY_3:
		*resultReceived++
	case communication.EOF_QUERY_4:
		*resultReceived++
	case communication.EOF_QUERY_5:
		*resultReceived++
	default:
		log.Errorf("action: receive_message_code_response | result: fail | client_id: %v | error: invalid message type | type: %v",
			c.config.ID,
			message.TypeMessage,
		)
	}
}

func (c *Client) handleQueryResult(queryNumber uint, message *communication.MessageProtocol) {
	if message.TypeMessage != communication.RESULT_QUERY_1 {
		c.resultQueries[int(queryNumber)] = result.NewResultQuery(int(queryNumber), string(message.Payload))
	} else {
		if _, exists := c.resultQueries[int(queryNumber)]; !exists {
			c.resultQueries[int(queryNumber)] = result.NewResultQuery(int(queryNumber), string(message.Payload))
		} else {
			// Append the result to the existing query result
			c.resultQueries[int(queryNumber)].AppendResult(string(message.Payload))
		}
	}
}

func (c *Client) SendFile(filename string, code int, codeEOF int, fileType string) {
	reader, err := NewFileReader(filename)
	if err != nil {
		log.Errorf("action: open_file | result: fail | error: %v", err)
		return
	}
	defer reader.Close()

	for {
		finishedSending := c.handleBatch(reader, code)
		if finishedSending {
			log.Infof("action: send_file %s | result: complete", fileType)
			break
		}
	}
	messageEOF := communication.NewMessageProtocol(
		// c.config.ID,
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
	return eof
}

func (c *Client) handleCloseConnection() {
	message := communication.NewMessageProtocol(
		// c.config.ID,
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
	// time.Sleep(5 * time.Second)
	if err := c.protocol.Close(); err != nil {
		log.Errorf("action: close_protocol | result: fail | error: %v", err)
		return
	}
}

func (c *Client) sendResultsToTester(payload string) {
	// Create a JSON object with client ID and results
	testerProtocol, err := communication.NewProtocol(c.config.TesterAddress)
	if err != nil {
		log.Criticalf("action: create_protocol_tester | result: fail | error: %v", err)
		return
	}

	defer testerProtocol.Close()

	messageWithClientID := map[string]interface{}{
		"client_id": c.config.ID,
		"results":   payload,
	}

	// Convert the map to JSON
	jsonData, err := json.Marshal(messageWithClientID)
	if err != nil {
		log.Errorf("action: format_results_to_json | result: fail | error: %v", err)
		return
	}

	// Create a message with the JSON results
	message := communication.NewMessageProtocol(
		communication.CLIENT_RESULTS,
		jsonData,
	)

	err = testerProtocol.SendMessage(message)
	if err != nil {
		log.Errorf("action: send_results_to_tester | result: fail | error: %v", err)
		return
	}

	// Wait for validation response
	response, err := testerProtocol.ReceiveMessage()
	if err != nil {
		log.Errorf("action: receive_validation | result: fail | error: %v", err)
		return
	}

	if response.TypeMessage == communication.RESULTS_VALIDATION {
		log.Infof("action: receive_validation | result: success | client_id: %v | validation: %s",
			c.config.ID,
			string(response.Payload),
		)
	}

	messageFinish := communication.NewMessageProtocol(
		communication.TYPE_FINISH_COMMUNICATION,
		nil,
	)
	errFinish := testerProtocol.SendMessage(messageFinish)
	if errFinish != nil {
		log.Errorf("action: send_message_code_end | result: fail | client_id: %v | error: %v",
			c.config.ID,
			errFinish,
		)
		return
	}
	log.Infof("action: send_results_to_tester | result: complete | client_id: %v", c.config.ID)
}
