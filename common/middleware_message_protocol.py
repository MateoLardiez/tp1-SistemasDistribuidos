import csv
from enum import Enum
import io
from common.defines import QueryNumber

SEPARATOR = "<|>"

class MiddlewareMessageType(Enum):
    MOVIES_BATCH = 1
    RATINGS_BATCH = 2
    CREDITS_BATCH = 3
    EOF_MOVIES = 4
    EOF_RATINGS = 5
    EOF_CREDITS = 6
    RESULT_Q1 = 7
    RESULT_Q2 = 8 
    RESULT_Q3 = 9
    RESULT_Q4 = 10
    RESULT_Q5 = 11
    EOF_JOINER = 12
    EOF_RESULT_Q1 = 13
    EOF_RESULT_Q2 = 14
    EOF_RESULT_Q3 = 15
    EOF_RESULT_Q4 = 16
    EOF_RESULT_Q5 = 17

class MiddlewareMessage:
    def __init__(self, query_number: int, client_id: int, seq_number: int, type: MiddlewareMessageType, payload: str = ""):
        self.query_number = QueryNumber(query_number)
        self.client_id = client_id
        self.seq_number = seq_number
        self.type = type
        self.payload = payload

    def encode_to_str(self) -> str:
        return f"{self.query_number.value}{SEPARATOR}{self.client_id}{SEPARATOR}{self.seq_number}{SEPARATOR}{self.type.value}{SEPARATOR}{self.payload}"
    
    @classmethod
    def decode_from_bytes(cls, raw_msg_body: bytes):
        msg = raw_msg_body.decode()
        query_number, client_id, seq_number, msg_type, payload  = msg.split(f"{SEPARATOR}")
        
        return cls(QueryNumber(int(query_number)), int(client_id), int(seq_number), MiddlewareMessageType(int(msg_type)), payload)
    
    def get_batch_iter_from_payload(self):
        return csv.reader(io.StringIO(self.payload), delimiter=',', quotechar='"')
        
    @classmethod
    def write_csv_batch(self, batch):
        output = io.StringIO()
        csv_writer = csv.writer(output, quoting=csv.QUOTE_NONNUMERIC, quotechar='"')
        csv_writer.writerows(batch)
        return output.getvalue().strip()
        