import logging

from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.defines import QueryNumber
from common.middleware_connection_handler import RabbitMQConnectionHandler

YEAR = 3 # release_date position
class JoinerByRatingId:
    year: int
    data: object

    def __init__(self):
        self.year_range_query_1 = (2000, 2009)
        self.year_range_query_3 = (2000, None)
        self.year_range_query_4 = (2000, None)
        
        self.data = ""
        self.filter_by_year_connection = RabbitMQConnectionHandler(
            producer_exchange_name="filter_by_year_exchange",
            producer_queues_to_bind={
                "filter_by_year_queue": ["filter_by_year_queue"],
            },
            consumer_exchange_name="filter_by_country_exchange",
            consumer_queues_to_recv_from=["country_queue"],
        )
        self.filter_by_year_connection.set_message_consumer_callback("country_queue", self.callback)

    def start(self):
        logging.info("action: start | result: success | code: filter_by_year")
        self.filter_by_year_connection.start_consuming()

    def callback(self, ch, method, properties, body):
        data = MiddlewareMessage.decode_from_bytes(body)
        if data.type != MiddlewareMessageType.EOF_MOVIES:
            lines = data.get_batch_iter_from_payload()
            if data.query_number == QueryNumber.ALL_QUERYS:
                self.handler_all_query(lines, data.query_number, data.client_id)
            elif data.query_number == QueryNumber.QUERY_1:
                self.handler_year_filter(lines, self.year_range_query_1, data.query_number, data.client_id)
        else:
            logging.info("action: EOF | result: success | code: filter_by_year")

    def handler_all_query(self, lines, query_number, client_id):
        self.handler_year_filter(lines, self.year_range_query_1, query_number, client_id)
        # self.handler_year_filter(lines, self.year_range_query_3)
        # self.handler_year_filter(lines, self.year_range_query_4)
        
    def filter_by_year(self, movie, year_filter):
        if len(movie) <= 3 or not movie[YEAR]:
            return False
        
        year_of_movie = movie[YEAR]
        try:
            release_year = int(year_of_movie.split('-')[0])
            if isinstance(year_filter, tuple):
                start, end = year_filter

                if start is not None and end is not None:
                    return start <= release_year <= end
                elif start is not None:
                    return release_year >= start
                elif end is not None:
                    return release_year <= end
                else:
                    return True  # (None, None) → no filtro
            else:
                return release_year == year_filter
        except (IndexError, ValueError):
            logging.error(f"Invalid release date format for movie: {movie}")
            return False

    def handler_year_filter(self, lines, year_filter, query_number, client_id):
        filtered_lines = []
        for line in lines:
            if self.filter_by_year(line, year_filter):
                filtered_lines.append(line)
                
        if filtered_lines:
            # Join all filtered lines into a single CSV string
            result_csv = MiddlewareMessage.write_csv_batch(filtered_lines)            
            msg = MiddlewareMessage(
                query_number=query_number,
                client_id=client_id,
                type=MiddlewareMessageType.MOVIES_BATCH,
                payload=result_csv
            )
            self.filter_by_year_connection.send_message(
                routing_key="filter_by_year_queue",
                msg_body=msg.encode_to_str()
            )
        
