import logging
from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.middleware_connection_handler import RabbitMQConnectionHandler
from common.resilient_node import ResilientNode

PROD_COUNTRIES = 5
BUDGET = 7

class FilterByCountryInvesment(ResilientNode):
    countries: list
    data: object

    def __init__(self, id_worker, number_workers):
        super().__init__()  # Call parent constructor
        self.id_worker = id_worker
        self.number_workers = number_workers
        self.rabbitmq_connection_handler = RabbitMQConnectionHandler(
            producer_exchange_name="filter_by_country_invesment_exchange",
            producer_queues_to_bind={
                **{f"filter_by_country_invesment_queue_{i}": [f"filter_by_country_invesment_queue_{i}"] for i in range(self.number_workers)}
            },
            consumer_exchange_name="movies_preprocessor_exchange",
            consumer_queues_to_recv_from=[f"cleaned_movies_queue_country_invesment_{self.id_worker}"],
        )
        self.rabbitmq_connection_handler.set_message_consumer_callback(f"cleaned_movies_queue_country_invesment_{self.id_worker}", self.callback)
        self.controller_name = f"filter_by_country_invesment_{id_worker}"
        self.clients_state = {}  # Dictionary to store local state of clients
        self.load_state()  # Load the state of clients from file

    def start(self):
        logging.info("action: start | result: success | code: filter_by_country")
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
                        routing_key=f"filter_by_country_invesment_queue_{id_worker}",
                        msg_body=msg.encode_to_str()
                    )
                
                del self.clients_state[data.client_id]
                self.save_state()
            return

        if data.client_id not in self.clients_state:
            self.clients_state[data.client_id] = {
                "last_seq_number": 0,  # This is the last seq number we propagated
                "eof_amount": 0 # This is the number of EOF messages received, when it reaches the number of workers, we can propagate the EOF message
            }
        if data.controller_name not in self.clients_state[data.client_id]:
            self.clients_state[data.client_id][data.controller_name] = data.seq_number  # This is the seq number we received
        elif data.seq_number <= self.clients_state[data.client_id][data.controller_name]:
            logging.warning(f"Duplicated Message {data.client_id} in {data.controller_name} with seq_number {data.seq_number}. Ignoring.")
            return
        
        if data.type != MiddlewareMessageType.EOF_MOVIES:        
            lines = data.get_batch_iter_from_payload()
            seq_number = self.clients_state[data.client_id]["last_seq_number"]
            self.handler_filter(lines, data.client_id, seq_number, data.query_number)

            self.clients_state[data.client_id]["last_seq_number"] += 1
            self.clients_state[data.client_id][data.controller_name] = data.seq_number  # Update the last seq number for this controller
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
                    # Send EOF message to all workers
                    self.rabbitmq_connection_handler.send_message(
                        routing_key=f"filter_by_country_invesment_queue_{id_worker}",
                        msg_body=msg.encode_to_str()
                    )
                del self.clients_state[data.client_id]  # Clean up state for this client
        self.save_state()  # Save state after processing each message

    def filter_by_country_invesment(self, movie):
        raw_value = movie[PROD_COUNTRIES].strip()

        if raw_value == "[]" or not raw_value:
            return False

        countries_of_movie = raw_value.strip("[]").replace("'", "").split(", ")
        countries_of_movie = [c for c in countries_of_movie if c.strip()]

        if len(countries_of_movie) == 1:
            return True
        return False

    def handler_filter(self, lines, client_id, seq_number, query_number):
        filtered_lines = []
        for line in lines:
            if self.filter_by_country_invesment(line):
                result_data = [line[PROD_COUNTRIES], line[BUDGET]]
                filtered_lines.append(result_data)

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
            routing_key=f"filter_by_country_invesment_queue_{id_worker}",
            msg_body=msg.encode_to_str()
        )
        
