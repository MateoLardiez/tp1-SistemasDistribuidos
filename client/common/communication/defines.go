package communication

// Common constants used across the application
const (
	MAX_BATCH_SIZE       = 8 * 1024
	CODE_AGENCY          = 'A'
	CODE_BATCH           = 'B'
	CODE_WAIT_FOR_RESULT = 'W'
	CODE_RESULT          = 'R'
	CODE_END             = 'E'
	CODE_WINNER          = 'S'
	CODE_QUERY           = 'Q'
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
	// File batch types
	BATCH_MOVIES  = 6
	BATCH_RATINGS = 7
	BATCH_CREDITS = 8
	BATCH_END     = 9
	// Communication types
	END_CODE = 10
)
