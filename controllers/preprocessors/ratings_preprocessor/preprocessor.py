import ast
import logging
from common.defines import QueryNumber
from common.middleware_connection_handler import RabbitMQConnectionHandler
from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType

ID = "id"
COLUMNS = ["movieId", "rating"]
COLUMNS_RATINGS =["userId", "movieId", "rating", "timestamp"]

class RatingsPreprocessor:
    countries: list
    data: object

    def __init__(self):
        self.ratings_preprocessor_connection = RabbitMQConnectionHandler(
            producer_exchange_name="raitngs_preprocessor_exchange",
            producer_queues_to_bind={
                "joiner_ratings_by_id_queue": ["joiner_ratings_by_id_queue"],
            },
            consumer_exchange_name="gateway_exchange",
            consumer_queues_to_recv_from=["ratings_queue"]
        )        
        # Configurar el callback para la cola específica
        self.ratings_preprocessor_connection.set_message_consumer_callback("ratings_queue", self.callback)

    def start(self):
        logging.info("action: start | result: success | code: ratings_preprocessor")
        self.ratings_preprocessor_connection.start_consuming()
    
    def callback(self, ch, method, properties, body):
        try:
            data = MiddlewareMessage.decode_from_bytes(body)
            if data.type != MiddlewareMessageType.EOF_RATINGS:
                lines = data.get_batch_iter_from_payload()
                clean_lines = self.clean_csv(lines)
                msg = MiddlewareMessage(
                    query_number=data.query_number,
                    client_id=data.client_id,
                    type=MiddlewareMessageType.MOVIES_BATCH,
                    payload=clean_lines,
                )
                self.ratings_preprocessor_connection.send_message(
                    routing_key="joiner_ratings_by_id_queue",
                    msg_body=msg.encode_to_str()
                )
                # logging.info(f"action: send_message | result: success | code: ratings_preprocessor | routing_key: joiner_ratings_by_id_queue")
            else:
                logging.info("action: EOF | result: success | code: ratings_preprocessor")
                msg = MiddlewareMessage(
                    query_number=data.query_number,
                    client_id=data.client_id,
                    type=MiddlewareMessageType.EOF_RATINGS,
                    payload=""
                )
                self.ratings_preprocessor_connection.send_message(
                    routing_key="joiner_ratings_by_id_queue",
                    msg_body=msg.encode_to_str()
                )
                
        except Exception as e:
            logging.error(f"action: error | result: failure | code: ratings_preprocessor | error: {e}")
        
    def clean_csv(self, reader):
        col_indices = {col: i for i, col in enumerate(COLUMNS_RATINGS) if col in COLUMNS}

        result = []

        for row in reader:
            if len(row) != len(COLUMNS_RATINGS):
                continue  # omitir filas mal formateadas

            row_dict = {col: row[col_indices[col]] for col in col_indices}
            
            filtered_row = [row_dict.get(col, '') for col in COLUMNS]

            result.append(filtered_row)
        
        return MiddlewareMessage.write_csv_batch(result)
