import pika
import logging
import csv
import io

from transformers import pipeline

from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.defines import QueryNumber
from common.middleware_connection_handler import RabbitMQConnectionHandler

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
            consumer_queues_to_recv_from=["aggregator_nlp_queue"]
        )
        self.aggregator_nlp_connection.set_message_consumer_callback("aggregator_nlp_queue", self.callback)

    def start(self):
        logging.info("action: start | result: success | code: aggregator_nlp")
        self.aggregator_nlp_connection.start_consuming()

    def callback(self, ch, method, properties, body):
        data = MiddlewareMessage.decode_from_bytes(body)
        lines = data.get_batch_iter_from_payload()
        if data.query_number == QueryNumber.QUERY_5:
            self.handler_filter_query_5(lines)
        
    def aggregator_nlp(self, movie):
        if (not movie[OVERVIEW]):
            return False, 0

        if (movie[BUDGET] == 0) or (movie[REVENUE] == 0):
            return False, 0

        sentiment_analyzer = pipeline('sentiment-analysis', model='distilbert-base-uncased-finetuned-sst-2-english')

        text = movie[OVERVIEW]
        
        try:
            truncated_text = text[:512]
            
            result = sentiment_analyzer(truncated_text)[0]  # Ej: {'label': 'POSITIVE', 'score': 0.998}
            
            return True, result['label']
        except (IndexError, ValueError):
            logging.error(f"Invalid release date format for movie: {movie}")
            return False, 0

    def handler_filter_query_5(self, lines):
        filtered_lines = []
        for line in lines:
            could_aggregate, value = self.aggregator_nlp(line)
            if could_aggregate:
                # Agregar el valor de sentimiento o POSITIVE o NEGATIVE a la linea
                line.append(value)
                filtered_lines.append(line)

        if filtered_lines:
            # Join all filtered lines into a single CSV string
            result_csv = '\n'.join([','.join(line) for line in filtered_lines])            
            msg = MiddlewareMessage(
                query_number=1,
                client_id=1,
                type=MiddlewareMessageType.MOVIES_BATCH,
                payload=result_csv
            )
            self.aggregator_nlp_connection.send_message(
                routing_key="aggregated_nlp_data_queue",
                msg_body=msg.encode_to_str()
            )
        
