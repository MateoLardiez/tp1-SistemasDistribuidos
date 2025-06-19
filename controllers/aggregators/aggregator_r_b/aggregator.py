import logging

from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.defines import QueryNumber
from common.middleware_connection_handler import RabbitMQConnectionHandler
from common.resilient_node import ResilientNode

BUDGET = 1
REVENUE = 2

# Columns needed: ["id", "title", "overview", "budget", "revenue"]

class AggregatorRB(ResilientNode):
    data: object
    def __init__(self, number_workers, worker_id):
        super().__init__()  # Call parent constructor
        self.worker_id = worker_id
        self.number_workers = number_workers
        self.controller_name = f"aggregator_r_b_{worker_id}"
        self.data = ""
        self.rabbitmq_connection_handler = RabbitMQConnectionHandler(
            producer_exchange_name="aggregator_r_b_exchange",
            producer_queues_to_bind={
                **{f"aggregated_r_b_data_queue_{i}": [f"aggregated_r_b_data_queue_{i}"] for i in range(self.number_workers)}
            },
            consumer_exchange_name="aggregator_nlp_exchange",
            consumer_queues_to_recv_from=[f"aggregated_nlp_data_queue_{self.worker_id}"],
        )
        self.rabbitmq_connection_handler.set_message_consumer_callback(f"aggregated_nlp_data_queue_{self.worker_id}", self.callback)
        self.clients_state = {}  # Dictionary to store local state of clients
        self.load_state()

    def start(self):
        logging.info("action: start | result: success | code: aggregator_r_b")
        try:
            self.rabbitmq_connection_handler.start_consuming()
        except Exception as e:
            logging.info("Consuming stopped")

    def callback(self, ch, method, properties, body):
        data = MiddlewareMessage.decode_from_bytes(body)
        if data.client_id not in self.clients_state:
            self.clients_state[data.client_id] = {
                "last_seq_number": 0,  # This is the last seq number we propagated
                "eof_amount": 0  # This is the number of EOF messages received, when it reaches the number of workers, we can propagate the EOF message
            }
        if data.controller_name not in self.clients_state[data.client_id]:
            self.clients_state[data.client_id][data.controller_name] = data.seq_number
        elif data.seq_number <= self.clients_state[data.client_id][data.controller_name]:
            logging.warning(f"Duplicated Message {data.client_id} in {data.controller_name} with seq_number {data.seq_number}. Ignoring.")
            return
        
        if data.type != MiddlewareMessageType.EOF_MOVIES:
            lines = data.get_batch_iter_from_payload()
            seq_number = self.clients_state[data.client_id]["last_seq_number"]
            self.handler_aggregator_query_5(lines, data.client_id, seq_number, data.query_number)

            self.clients_state[data.client_id]["last_seq_number"] += 1
            self.clients_state[data.client_id][data.controller_name] = data.seq_number
        else:
            seq_number = self.clients_state[data.client_id]["last_seq_number"]
            self.clients_state[data.client_id]["eof_amount"] += 1
            if self.clients_state[data.client_id]["eof_amount"] == self.number_workers:
                msg = MiddlewareMessage(
                    query_number=data.query_number,
                    client_id=data.client_id,
                    seq_number=seq_number,
                    type=MiddlewareMessageType.EOF_MOVIES,
                    payload="",
                    controller_name=self.controller_name
                )
                for id_worker in range(self.number_workers):
                    self.rabbitmq_connection_handler.send_message(
                        routing_key=f"aggregated_r_b_data_queue_{id_worker}",
                        msg_body=msg.encode_to_str()
                    )
                del self.clients_state[data.client_id]
        self.save_state()
        
    def aggregator_r_b(self, movie):
        try:
            revenue = float(movie[REVENUE])
            budget = float(movie[BUDGET])
            rate = revenue / budget
            
            return True, rate
        except (IndexError, ValueError):
            logging.error(f"Invalid release date format for movie: {movie}")
            return False, 0

    def handler_aggregator_query_5(self, lines, client_id, seq_number, query_number):
        filtered_lines = []
        for line in lines:
            could_aggregate, rate_value = self.aggregator_r_b(line)
            if could_aggregate:
                filtered_line = []
                # Agregar el rate
                filtered_line.append(line[0])
                filtered_line.append(str(rate_value))
                filtered_lines.append(filtered_line)

        # if filtered_lines:
            # Join all filtered lines into a single CSV string
        result_csv = MiddlewareMessage.write_csv_batch(filtered_lines)
        msg = MiddlewareMessage(
            query_number=query_number,
            client_id=client_id,
            seq_number=seq_number,
            type=MiddlewareMessageType.MOVIES_BATCH,
            payload=result_csv, 
            controller_name=self.controller_name
        )
        id_worker = seq_number % self.number_workers
        self.rabbitmq_connection_handler.send_message(
            routing_key=f"aggregated_r_b_data_queue_{id_worker}",
            msg_body=msg.encode_to_str()
        )
        
