package communication

// Common constants used across the application
const (
	MAX_BATCH_SIZE = 64 * 1024
	// CODE_AGENCY          = 'A'
	// CODE_BATCH           = 'B'
	// CODE_WAIT_FOR_RESULT = 'W'
	// CODE_RESULT          = 'R'
	// CODE_WINNER          = 'S'

	CODE_END      = 'E'
	CODE_QUERY    = 'Q'
	CODE_RESPONSE = 'R'

	// Size of the header and code in bytes
	SIZE_HEADER = 4
	SIZE_CODE   = 1
	// Query types
	ALL_QUERYS = 0
	QUERY_1    = 1
	QUERY_2    = 2
	QUERY_3    = 3
	QUERY_4    = 4
	QUERY_5    = 5
	// Communication types
	// END_CODE = 10

	// Types Message
	TYPE_QUERY                = 0
	TYPE_ACK                  = 1
	TYPE_FINISH_COMMUNICATION = 2
	// File batch types
	BATCH_MOVIES  = 3
	BATCH_RATINGS = 4
	BATCH_CREDITS = 5
	// Eof files
	EOF_MOVIES  = 6
	EOF_RATINGS = 7
	EOF_CREDITS = 8
	// Result types
	RESULT_QUERY_1 = 9
	EOF_QUERY_1    = 10
	RESULT_QUERY_2 = 11
	EOF_QUERY_2    = 12
	RESULT_QUERY_3 = 13
	EOF_QUERY_3    = 14
	RESULT_QUERY_4 = 15
	EOF_QUERY_4    = 16
	RESULT_QUERY_5 = 17
	EOF_QUERY_5    = 18
	//
	FINISH_SEND_FILES = 19
	TYPE_ERROR        = 20
	// Results testing
	CLIENT_RESULTS     = 21
	RESULTS_VALIDATION = 22

	// Init communication
	TYPE_INIT = 23
	CODE_INIT = 24
)
