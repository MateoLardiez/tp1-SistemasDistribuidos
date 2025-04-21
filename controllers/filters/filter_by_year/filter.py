import pika
import logging
import csv
import io


from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.defines import QueryNumber
from common.middleware_connection_handler import RabbitMQConnectionHandler

YEAR = 3 # release_date position
ID = "id"

class FilterByYear:
    year: int
    data: object

    def __init__(self):
        self.year = 2000
        self.data = ""
        self.filter_by_year_connection = RabbitMQConnectionHandler(
            producer_exchange_name="filter_by_year_exchange",
            producer_queues_to_bind={"filter_by_year_queue": ["filter_by_year_queue"]},
            consumer_exchange_name="filter_by_country_exchange",
            consumer_queues_to_recv_from=["filter_by_country_queue"],
        )
        self.filter_by_year_connection.set_message_consumer_callback("filter_by_country_queue", self.callback)

    def start(self):
        logging.info("action: start | result: success | code: filter_by_year")
        self.filter_by_year_connection.start_consuming()

    def callback(self, ch, method, properties, body):
        data = MiddlewareMessage.decode_from_bytes(body)
        lines = data.get_batch_iter_from_payload()
        if data.query_number == QueryNumber.QUERY_1:
            self.handler_filter_query_1(lines)
        
    def filter_by_year(self, movie):
        if len(movie) <= 3 or not movie[YEAR]:
            return False
        
        year_of_movie = movie[YEAR]
        try:
            release_year = int(year_of_movie.split('-')[0])
            return (release_year >= 2000)
        except (IndexError, ValueError):
            logging.error(f"Invalid release date format for movie: {movie}")
            return False

    def handler_filter_query_1(self, lines):
        filtered_lines = []
        for line in lines:
            if self.filter_by_year(line):
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
            self.filter_by_year_connection.send_message(
                routing_key="filter_by_year_queue",
                msg_body=msg.encode_to_str()
            )
        
