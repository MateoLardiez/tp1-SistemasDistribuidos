import logging
from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.middleware_connection_handler import RabbitMQConnectionHandler
from common.resilient_node import ResilientNode
from common.file_manager import FileManager

class Query4(ResilientNode):

    def __init__(self, id_sinker, number_workers):
        super().__init__()
        self.rabbitmq_connection_handler = RabbitMQConnectionHandler(
            producer_exchange_name="reports_exchange",
            producer_queues_to_bind={"reports_queue": ["reports_queue"]},
            consumer_exchange_name="joiner_by_credit_id_exchange",
            consumer_queues_to_recv_from=[f"average_credit_aggregated_{id_sinker}"],
        )
        self.rabbitmq_connection_handler.set_message_consumer_callback(f"average_credit_aggregated_{id_sinker}", self.callback)
        self.number_workers = number_workers
        self.clients_state = {}
        self.controller_name = f"sink_query_4_{id_sinker}"
        self.load_state(self.check_files_state)  # Load the state of clients from file

    def start(self):
        logging.info("action: start | result: success | code: Sink_query_4 ")
        try:
            self.rabbitmq_connection_handler.start_consuming()
        except Exception as e:
            logging.info(f"Consuming stopped")

    def check_files_state(self):
        for client_id in self.clients_state:
            if self.check_file(client_id, "query_4"):
                self.clients_state[client_id]["duplicated_batch"]["query_4"] = True

    def callback(self, ch, method, properties, body):
        # id,title,genres,release_date,overview,production_countries,spoken_languages,budget,revenue
        data = MiddlewareMessage.decode_from_bytes(body)

        if data.type == MiddlewareMessageType.ABORT:
            logging.info(f"Received ABORT message from client {data.client_id}. Stopping processing.")
            if data.client_id in self.clients_state:
                files_to_remove = [
                    f".data/query_4-client-{data.client_id}",
                ]
                FileManager.clean_temp_files(files_to_remove)
                del self.clients_state[data.client_id]
                self.save_state()
            return

        if data.client_id not in self.clients_state:
            self.clients_state[data.client_id] = {
                "eof_amount": 0, 
                "last_seq_number": 0,
                "hash_file": {
                    "query_4": None,
                },
                "duplicated_batch": {
                    "query_4": False,
                },
            }
        if data.controller_name not in self.clients_state[data.client_id]:
            self.clients_state[data.client_id][data.controller_name] = data.seq_number
        elif self.clients_state[data.client_id]["duplicated_batch"]["query_4"]:
            logging.warning(f"Duplicated batch for client {data.client_id} in {data.controller_name}. Ignoring.")
            self.update_duplicate_state(
                data.client_id, "query_4", data.controller_name, data.seq_number
            )
            return
        elif data.seq_number <= self.clients_state[data.client_id][data.controller_name]:
            logging.warning(f"Duplicated Message {data.client_id} in {data.controller_name} with seq_number {data.seq_number}. Ignoring.")
            return
        
        if data.type != MiddlewareMessageType.EOF_JOINER: 
            lines = data.get_batch_iter_from_payload()
            filename = f".data/query_4-client-{data.client_id}"
            self.clients_state[data.client_id]["hash_file"]["query_4"] = self.save_data(filename, lines)
            self.clients_state[data.client_id][data.controller_name] = data.seq_number  # Update
        else:
            self.clients_state[data.client_id]["eof_amount"] += 1
            self.clients_state[data.client_id][data.controller_name] = data.seq_number  # Update the last seq number for this controller
            if self.clients_state[data.client_id]["eof_amount"] == self.number_workers:
                self.handler_query_4(data.client_id, data.query_number)
                # Handle EOF message
                msg = MiddlewareMessage(
                    query_number=data.query_number,
                    client_id=data.client_id,
                    seq_number=0,
                    type=MiddlewareMessageType.EOF_RESULT_Q4,
                    payload="EOF",
                    controller_name=self.controller_name
                )
                self.rabbitmq_connection_handler.send_message(
                    routing_key="reports_queue",
                    msg_body=msg.encode_to_str()
                )

                files_to_remove = [
                    f".data/query_4-client-{data.client_id}",
                ]
                FileManager.clean_temp_files(files_to_remove)
                del self.clients_state[data.client_id]
        self.save_state()
        
    def handler_query_4(self, client_id, query_number):
        # Ya tengo toda la data en mi csv
        actors = {}
        filename = f".data/query_4-client-{client_id}"
        for line in self.read_data(filename):
            actor = line[0]
            if actor not in actors:
                actors[actor] = 0
            actors[actor] += int(line[1])  # Cantidad de creditos

        # Ordenar por cantidad de apariciones y luego por orden alfabetico de artistas
        sorted_actors = sorted(actors.items(), key=lambda x: (-x[1], x[0]))
        
        # Limitar a 10 resultados
        result = [[actor, count] for actor, count in sorted_actors[:10]]

        result_csv = MiddlewareMessage.write_csv_batch(result)
        
        msg = MiddlewareMessage(
            query_number=query_number,
            client_id=client_id,
            seq_number=0,
            type=MiddlewareMessageType.RESULT_Q4,
            payload=result_csv,
            controller_name=self.controller_name
        )
        # Send all filtered results in a single message
        self.rabbitmq_connection_handler.send_message(
            routing_key="reports_queue",
            msg_body=msg.encode_to_str()
        )

    def save_data(self, filename, lines) -> None:
        writer = FileManager(filename)
        return writer.save_data(filename, lines)

    def read_data(self, filename):
        reader = FileManager(filename)
        return reader.read()