from transformers import pipeline
from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.defines import QueryNumber
from common.middleware_connection_handler import RabbitMQConnectionHandler
from common.resilient_node import ResilientNode
import logging
import torch

torch.set_num_threads(1)  

ID = "id"
OVERVIEW = 4
BUDGET = 7
REVENUE = 8

class AggregatorNlp(ResilientNode):
    data: object
    def __init__(self, number_workers, worker_id):
        super().__init__()
        self.worker_id = worker_id
        self.number_workers = number_workers
        self.data = ""
        self.rabbitmq_connection_handler = RabbitMQConnectionHandler(
            producer_exchange_name="aggregator_nlp_exchange",
            producer_queues_to_bind={
                **{f"aggregated_nlp_data_queue_{i}": [f"aggregated_nlp_data_queue_{i}"] for i in range(self.number_workers)}
            },
            consumer_exchange_name="movies_preprocessor_exchange",
            consumer_queues_to_recv_from=[f"cleaned_movies_queue_nlp_{self.worker_id}"],
        )
        self.rabbitmq_connection_handler.set_message_consumer_callback(f"cleaned_movies_queue_nlp_{self.worker_id}", self.callback)
        self.sentiment_analyzer = pipeline('sentiment-analysis', model='distilbert-base-uncased-finetuned-sst-2-english')
        self.clients_state = {}  # Dictionary to store local state of clients
        self.controller_name = f"aggregator_nlp_{worker_id}"
        self.load_state()
    
    def start(self):
        logging.info("action: start | result: success | code: aggregator_nlp")
        try:
            self.rabbitmq_connection_handler.start_consuming()
        except Exception as e:
            logging.info("Consuming stopped")

    def callback(self, ch, method, properties, body):
        data = MiddlewareMessage.decode_from_bytes(body)
        # if data.type == MiddlewareMessageType.ABORT:
        #     logging.info(f"Received ABORT message from client {data.client_id}. Stopping processing.")
        #     return

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
            self.handler_aggregator_query_5(lines, data.client_id, data.query_number, seq_number)
            self.clients_state[data.client_id]["last_seq_number"] += 1
            self.clients_state[data.client_id][data.controller_name] = data.seq_number  # Update the last seq number for this controller
        else:
            seq_number = self.clients_state[data.client_id]["last_seq_number"]
            self.clients_state[data.client_id]["eof_amount"] += 1
            if self.clients_state[data.client_id]["eof_amount"] == self.number_workers:
                msg = MiddlewareMessage(
                    query_number=data.query_number,
                    client_id=data.client_id,
                    type=MiddlewareMessageType.EOF_MOVIES,
                    seq_number=seq_number,
                    payload="",
                    controller_name=self.controller_name
                )
                for id_worker in range(self.number_workers):
                    # Send the EOF message to all workers
                    self.rabbitmq_connection_handler.send_message(
                        routing_key=f"aggregated_nlp_data_queue_{id_worker}",
                        msg_body=msg.encode_to_str()
                    )
                del self.clients_state[data.client_id]
        self.save_state()

    def aggregator_nlp(self, movie):
        if (not movie[OVERVIEW]):
            return False, 0

        try:
            budget = float(movie[BUDGET])
            revenue = float(movie[REVENUE])
            if budget <= 0 or revenue <= 0:
                return False, 0
        except (ValueError, TypeError):
            return False, 0

        text = movie[OVERVIEW]
        
        try:
            truncated_text = text[:512]
            
            result = self.sentiment_analyzer(truncated_text)[0]  # Ej: {'label': 'POSITIVE', 'score': 0.998}
            
            return True, result['label']
        except (IndexError, ValueError):
            logging.error(f"Invalid release date format for movie: {movie}")
            return False, 0

    def handler_aggregator_query_5(self, lines, client_id, query_number, seq_number):
        filtered_lines = []
        for line in lines:
            could_aggregate, sentiment_value = self.aggregator_nlp(line)
            if could_aggregate:
                filtered_line = []
                filtered_line.append(sentiment_value)
                filtered_line.append(line[BUDGET])
                filtered_line.append(line[REVENUE])
                filtered_lines.append(filtered_line)

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
            routing_key=f"aggregated_nlp_data_queue_{id_worker}",
            msg_body=msg.encode_to_str()
        )

