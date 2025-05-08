import logging

from transformers import pipeline

from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.defines import QueryNumber
from common.middleware_connection_handler import RabbitMQConnectionHandler

import torch

torch.set_num_threads(1)  

ID = "id"
OVERVIEW = 4
BUDGET = 7
REVENUE = 8

# Columns needed: ["id", "title", "overview", "budget", "revenue"]

class AggregatorNlp:
    data: object
    def __init__(self):
        self.data = ""
        self.aggregator_nlp_connection = RabbitMQConnectionHandler(
            producer_exchange_name="aggregator_nlp_exchange",
            producer_queues_to_bind={ "aggregated_nlp_data_queue": ["aggregated_nlp_data_queue"]},
            consumer_exchange_name="movies_preprocessor_exchange",
            consumer_queues_to_recv_from=["cleaned_movies_queue_nlp"]
        )
        self.aggregator_nlp_connection.set_message_consumer_callback("cleaned_movies_queue_nlp", self.callback)
        self.sentiment_analyzer = pipeline('sentiment-analysis', model='distilbert-base-uncased-finetuned-sst-2-english')

    def start(self):
        logging.info("action: start | result: success | code: aggregator_nlp")
        self.aggregator_nlp_connection.start_consuming()

    def callback(self, ch, method, properties, body):
        data = MiddlewareMessage.decode_from_bytes(body)

        if data.type != MiddlewareMessageType.EOF_MOVIES:
            lines = data.get_batch_iter_from_payload()
            self.handler_aggregator_query_5(lines, data.client_id, data.query_number, data.seq_number)
        else:
            msg = MiddlewareMessage(
                query_number=data.query_number,
                client_id=data.client_id,
                type=MiddlewareMessageType.EOF_MOVIES,
                seq_number=data.seq_number,
                payload=""
            )
            self.aggregator_nlp_connection.send_message(
                routing_key="aggregated_nlp_data_queue",
                msg_body=msg.encode_to_str()
            )

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
            payload=result_csv
        )
        self.aggregator_nlp_connection.send_message(
            routing_key="aggregated_nlp_data_queue",
            msg_body=msg.encode_to_str()
        )

