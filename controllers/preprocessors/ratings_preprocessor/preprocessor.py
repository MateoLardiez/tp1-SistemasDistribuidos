import ast
import logging
from common.defines import QueryNumber
from common.middleware_connection_handler import RabbitMQConnectionHandler
from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.resilient_node import ResilientNode

ID = 0
COLUMNS = ["movieId", "rating"]
COLUMNS_RATINGS =["userId", "movieId", "rating", "timestamp"]

class RatingsPreprocessor(ResilientNode):

    def __init__(self, number_workers, id_worker):
        super().__init__()  # Call parent constructor
        self.id_worker = id_worker
        self.rabbitmq_connection_handler = RabbitMQConnectionHandler(
            producer_exchange_name="ratings_preprocessor_exchange",
            producer_queues_to_bind={
                **{f"joiner_ratings_by_id_queue_{i}": [f"joiner_ratings_by_id_queue_{i}"] for i in range(number_workers)},
            },
            consumer_exchange_name="gateway_exchange",
            consumer_queues_to_recv_from=[f"ratings_queue_{id_worker}"]
        )
        # Configurar el callback para la cola específica
        self.rabbitmq_connection_handler.set_message_consumer_callback(f"ratings_queue_{id_worker}", self.callback)
        self.number_workers = number_workers
        self.controller_name = f"ratings_preprocessor_{id_worker}"
        self.clients_state = {}  # Diccionario para almacenar el estado local de los clientes
        self.load_state()  # Cargar el estado de los clientes desde el archivo

    def start(self):
        logging.info("action: start | result: success | code: ratings_preprocessor")
        try:
            self.rabbitmq_connection_handler.start_consuming()
        except Exception as e:
            logging.info("Consuming stopped")

    def callback(self, ch, method, properties, body):
        data = MiddlewareMessage.decode_from_bytes(body)

        if data.type == MiddlewareMessageType.ABORT:
            logging.info(f"Received ABORT message from client {data.client_id}. Stopping processing.")
            if data.client_id in self.clients_state:
                msg = MiddlewareMessage(
                    query_number=data.query_number,
                    client_id=data.client_id,
                    type=MiddlewareMessageType.ABORT,
                    seq_number=data.seq_number,
                    payload="",
                    controller_name=self.controller_name
                )
                for id_worker in range(self.number_workers):
                    # Send the ABORT message to all workers
                    self.rabbitmq_connection_handler.send_message(
                        routing_key=f"joiner_ratings_by_id_queue_{id_worker}",
                        msg_body=msg.encode_to_str()
                    )
                
                del self.clients_state[data.client_id]
                self.save_state()
            return

        if data.client_id not in self.clients_state:
            self.clients_state[data.client_id] = {
                "last_seq_number": 0  # Este es el último seq number que propagamos
            }
        if data.controller_name not in self.clients_state[data.client_id]:
            self.clients_state[data.client_id][data.controller_name] = data.seq_number
        elif data.seq_number <= self.clients_state[data.client_id][data.controller_name]:
            logging.warning(f"Duplicated Message {data.client_id} in {data.controller_name} with seq_number {data.seq_number}. Ignoring.")
            return
    
        if data.type != MiddlewareMessageType.EOF_RATINGS:
            lines = data.get_batch_iter_from_payload()
            clean_lines = self.clean_csv(lines)
            seq_number = self.clients_state[data.client_id]["last_seq_number"]
            for sharding_id, data_shard in clean_lines.items():
                data_csv = MiddlewareMessage.write_csv_batch(data_shard)
                msg = MiddlewareMessage(
                    query_number=data.query_number,
                    client_id=data.client_id,
                    seq_number=seq_number,
                    type=MiddlewareMessageType.RATINGS_BATCH,
                    payload=data_csv,
                    controller_name=self.controller_name
                )
                self.rabbitmq_connection_handler.send_message(
                    routing_key=f"joiner_ratings_by_id_queue_{sharding_id}",
                    msg_body=msg.encode_to_str()
                )
            self.clients_state[data.client_id]["last_seq_number"] += 1
            self.clients_state[data.client_id][data.controller_name] = data.seq_number
        else:
            seq_number = self.clients_state[data.client_id]["last_seq_number"]
            for i in range(self.number_workers):
                msg = MiddlewareMessage(
                    query_number=data.query_number,
                    client_id=data.client_id,
                    seq_number=seq_number,
                    type=MiddlewareMessageType.EOF_RATINGS,
                    payload="",
                    controller_name=self.controller_name
                )
                self.rabbitmq_connection_handler.send_message(
                    routing_key=f"joiner_ratings_by_id_queue_{i}",
                    msg_body=msg.encode_to_str()
                )
            del self.clients_state[data.client_id]
        self.save_state()
                 
    def clean_csv(self, reader):
        col_indices = {col: i for i, col in enumerate(COLUMNS_RATINGS) if col in COLUMNS}

        result = {}

        for row in reader:
            if len(row) != len(COLUMNS_RATINGS):
                continue  # omitir filas mal formateadas

            row_dict = {col: row[col_indices[col]] for col in col_indices}
            
            filtered_row = [row_dict.get(col, '') for col in COLUMNS]

            sharding_key = int(filtered_row[ID]) % self.number_workers
            if sharding_key not in result:
                result[sharding_key] = []
            result[sharding_key].append(filtered_row)
        
        return result
