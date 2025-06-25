import logging
from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.middleware_connection_handler import RabbitMQConnectionHandler
from common.resilient_node import ResilientNode

TITLE = 0
GENRES = 1

class Query1(ResilientNode):

    def __init__(self, id_sinker, number_workers):
        super().__init__()
        self.number_workers = number_workers
        self.rabbitmq_connection_handler = RabbitMQConnectionHandler(
            producer_exchange_name="reports_exchange",
            producer_queues_to_bind={"reports_queue": ["reports_queue"]},
            consumer_exchange_name="filter_by_year_exchange",
            consumer_queues_to_recv_from=[f"sink_query_1_queue_{id_sinker}"],
        )
        self.rabbitmq_connection_handler.set_message_consumer_callback(f"sink_query_1_queue_{id_sinker}", self.callback)
        self.clients_state = {}
        self.controller_name = f"sink_query_1_{id_sinker}"
        self.load_state()  # Load the state of clients from file

    def start(self):
        logging.info("action: start | result: success | code: Sink_query_1 ")
        try:
            self.rabbitmq_connection_handler.start_consuming()
        except Exception as e:
            logging.info("Consuming stopped")

    def callback(self, ch, method, properties, body):
        # id,title,genres,release_date,overview,production_countries,spoken_languages,budget,revenue
        data = MiddlewareMessage.decode_from_bytes(body)

        if data.type == MiddlewareMessageType.ABORT:
            logging.info(f"Received ABORT message from client {data.client_id}. Stopping processing.")
            if data.client_id in self.clients_state:
                del self.clients_state[data.client_id]
                self.save_state()
            return

        if data.client_id not in self.clients_state:
            self.clients_state[data.client_id] = {
                "eof_amount": 0, 
                "last_seq_number": 0, 
            }
        if data.controller_name not in self.clients_state[data.client_id]:
            self.clients_state[data.client_id][data.controller_name] = data.seq_number
        elif data.seq_number <= self.clients_state[data.client_id][data.controller_name]:
            logging.warning(f"Duplicated Message {data.client_id} in {data.controller_name} with seq_number {data.seq_number}. Ignoring.")
            return
        
        if data.type != MiddlewareMessageType.EOF_MOVIES:    
            lines = data.get_batch_iter_from_payload()
            seq_number = self.clients_state[data.client_id]["last_seq_number"]
            self.handler_query_1(lines, data.client_id, data.query_number, seq_number)
            self.clients_state[data.client_id]["last_seq_number"] += 1
            self.clients_state[data.client_id][data.controller_name] = data.seq_number  # Update the last seq number for this controller
        else:
            self.clients_state[data.client_id]["eof_amount"] += 1
            self.clients_state[data.client_id][data.controller_name] = data.seq_number
            if self.clients_state[data.client_id]["eof_amount"] == self.number_workers:
                msg = MiddlewareMessage(
                    query_number=data.query_number,
                    client_id=data.client_id,
                    seq_number=0,
                    type=MiddlewareMessageType.EOF_RESULT_Q1,
                    payload="EOF",
                    controller_name=self.controller_name
                )
                self.rabbitmq_connection_handler.send_message(
                    routing_key="reports_queue",
                    msg_body=msg.encode_to_str()
                )
                del self.clients_state[data.client_id]
        self.save_state()  # Save the state of clients to file

    def handler_query_1(self, lines, client_id, query_number, seq_number):
        filtered_lines = []
        for line in lines:
            filtered_lines.append([line[TITLE], line[GENRES]])
        
        if filtered_lines:
            # Join all filtered lines into a single CSV string
            result_csv = MiddlewareMessage.write_csv_batch(filtered_lines)
            
            msg = MiddlewareMessage(
                query_number=query_number,
                client_id=client_id,
                seq_number=seq_number,
                type=MiddlewareMessageType.RESULT_Q1,
                payload=result_csv,
                controller_name=self.controller_name
            )

            # Send all filtered results in a single message
            self.rabbitmq_connection_handler.send_message(
                routing_key="reports_queue",
                msg_body=msg.encode_to_str()
            )
     
