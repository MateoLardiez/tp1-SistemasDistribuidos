from enum import Enum

class QueryNumber(Enum):
    QUERY_1 = 1
    QUERY_2 = 2
    QUERY_3 = 3
    QUERY_4 = 4
    QUERY_5 = 5


class ClientCommunication(Enum):
    ALL_QUERYS = 0
    QUERY_1    = 1
    QUERY_2    = 2
    QUERY_3    = 3
    QUERY_4    = 4
    QUERY_5    = 5
    # Communication types
    #  END_CODE = 10

    # Types Message
    TYPE_QUERY                = 0
    TYPE_ACK                  = 1
    TYPE_FINISH_COMMUNICATION = 2
    # File batch types
    BATCH_MOVIES  = 3
    BATCH_RATINGS = 4
    BATCH_CREDITS = 5
    # Eof files
    EOF_MOVIES  = 6
    EOF_RATINGS = 7
    EOF_CREDITS = 8
    # Result types
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
    # FINISH SEND FILES
    FINISH_SEND_FILES = 19
