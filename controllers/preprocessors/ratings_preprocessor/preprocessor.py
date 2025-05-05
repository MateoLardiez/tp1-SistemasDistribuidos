import ast
import logging
from common.defines import QueryNumber
from common.middleware_connection_handler import RabbitMQConnectionHandler
from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType

ID = 0
COLUMNS = ["movieId", "rating"]
COLUMNS_RATINGS =["userId", "movieId", "rating", "timestamp"]

class RatingsPreprocessor:

    def __init__(self, numberWorkers):
        self.ratings_preprocessor_connection = RabbitMQConnectionHandler(
            producer_exchange_name="ratings_preprocessor_exchange",
            producer_queues_to_bind={
                **{f"joiner_ratings_by_id_queue_{i}": [f"joiner_ratings_by_id_queue_{i}"] for i in range(numberWorkers)},
                },
                # "joiner_ratings_by_id_queue": ["joiner_ratings_by_id_queue"],
            consumer_exchange_name="gateway_exchange",
            consumer_queues_to_recv_from=["ratings_queue"]
        )        
        # Configurar el callback para la cola específica
        self.ratings_preprocessor_connection.set_message_consumer_callback("ratings_queue", self.callback)
        self.numberWorkers = numberWorkers

    def start(self):
        logging.info("action: start | result: success | code: ratings_preprocessor")
        self.ratings_preprocessor_connection.start_consuming()
    
    def callback(self, ch, method, properties, body):
        try:
            data = MiddlewareMessage.decode_from_bytes(body)
            if data.type != MiddlewareMessageType.EOF_RATINGS:
                lines = data.get_batch_iter_from_payload()
                clean_lines = self.clean_csv(lines)
                for sharding_id, data_shard in clean_lines.items():
                    data_csv = MiddlewareMessage.write_csv_batch(data_shard)
                    msg = MiddlewareMessage(
                        query_number=data.query_number,
                        client_id=data.client_id,
                        type=MiddlewareMessageType.RATINGS_BATCH,
                        payload=data_csv,
                    )
                    self.ratings_preprocessor_connection.send_message(
                        routing_key=f"joiner_ratings_by_id_queue_{sharding_id}",
                        msg_body=msg.encode_to_str()
                    )
            else:
                logging.info("action: EOF | result: success | code: ratings_preprocessor")
                for i in range(self.numberWorkers):
                    msg = MiddlewareMessage(
                        query_number=data.query_number,
                        client_id=data.client_id,
                        type=MiddlewareMessageType.EOF_RATINGS,
                        payload=""
                    )
                    self.ratings_preprocessor_connection.send_message(
                        routing_key=f"joiner_ratings_by_id_queue_{i}",
                        msg_body=msg.encode_to_str()
                    )
                
        except Exception as e:
            logging.error(f"action: error | result: failure | code: ratings_preprocessor | error: {e}")
        
    def clean_csv(self, reader):
        col_indices = {col: i for i, col in enumerate(COLUMNS_RATINGS) if col in COLUMNS}

        result = {}

        for row in reader:
            if len(row) != len(COLUMNS_RATINGS):
                continue  # omitir filas mal formateadas

            row_dict = {col: row[col_indices[col]] for col in col_indices}
            
            filtered_row = [row_dict.get(col, '') for col in COLUMNS]

            sharding_key = int(filtered_row[ID]) % self.numberWorkers
            if sharding_key not in result:
                result[sharding_key] = []
            result[sharding_key].append(filtered_row)
        
        return result
