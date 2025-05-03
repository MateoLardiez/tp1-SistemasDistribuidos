import logging

from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.defines import QueryNumber
from common.middleware_connection_handler import RabbitMQConnectionHandler

YEAR_Q1 = 2 # release_date position
YEAR_Q3 = 2 # release_date position for query 3
YEAR_Q4 = 1 # release_date position for query 4
class FilterByYear:
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
                "sink_query_1_queue": ["sink_query_1_queue"],
                "joiner_by_ratings_movies_queue": ["joiner_by_ratings_movies_queue"],
                "joiner_by_credits_movies_queue": ["joiner_by_credits_movies_queue"],
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
            if data.query_number == QueryNumber.QUERY_1:
                self.handler_year_filter(lines, self.year_range_query_1, data.query_number, data.client_id, YEAR_Q1)
            elif data.query_number == QueryNumber.QUERY_3:
                self.handler_year_filter(lines, self.year_range_query_3, data.query_number, data.client_id, YEAR_Q3)
            elif data.query_number == QueryNumber.QUERY_4:
                self.handler_year_filter(lines, self.year_range_query_4, data.query_number, data.client_id, YEAR_Q4)
        else:
            logging.info("action: EOF | result: success | code: filter_by_year")
            msg = MiddlewareMessage(
                query_number=data.query_number,
                client_id=data.client_id,
                type=MiddlewareMessageType.EOF_MOVIES,
                payload=""
            )
            if data.query_number == QueryNumber.QUERY_1:
               self.filter_by_year_connection.send_message(
                    routing_key="sink_query_1_queue",
                    msg_body=msg.encode_to_str()
               )
            elif data.query_number == QueryNumber.QUERY_3:
                self.filter_by_year_connection.send_message(
                    routing_key="joiner_by_ratings_movies_queue",
                    msg_body=msg.encode_to_str()
                )
            elif data.query_number == QueryNumber.QUERY_4:
                self.filter_by_year_connection.send_message(
                    routing_key="joiner_by_credits_movies_queue",
                           msg_body=msg.encode_to_str()
                       )
    
    def filter_by_year(self, movie, year_filter, year_pos):
        if not movie[year_pos]:
            return False
        
        year_of_movie = movie[year_pos]
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

    def handler_year_filter(self, lines, year_filter, query_number, client_id, year_pos):
        filtered_lines = []
        for line in lines:
            if self.filter_by_year(line, year_filter, year_pos):
                filtered_lines.append(line)
                
        query_result = []
        if filtered_lines:
            # Entradas
            # Q1: [title, genres, release_date]
            # Q3: [id, title, release_date]
            # Q4: [id, release_date]
            # Salidas:
            # Q1: [title, genres]
            # Q3: [id, title]
            # Q4: [id]
            if query_number == QueryNumber.QUERY_1:
                for line in filtered_lines:
                    query_result.append([line[0], line[1]])
            elif query_number == QueryNumber.QUERY_3:
                for line in filtered_lines:
                    query_result.append([line[0], line[1]])
            elif query_number == QueryNumber.QUERY_4:
                for line in filtered_lines:
                    query_result.append([line[0]])

        # Join all filtered lines into a single CSV string
        result_csv = MiddlewareMessage.write_csv_batch(query_result)            
        msg = MiddlewareMessage(
            query_number=query_number,
            client_id=client_id,
            type=MiddlewareMessageType.MOVIES_BATCH,
            payload=result_csv
        )

        if query_number == QueryNumber.QUERY_1:
            self.filter_by_year_connection.send_message(
                routing_key="sink_query_1_queue",
                msg_body=msg.encode_to_str()
            )
        elif query_number == QueryNumber.QUERY_3:
            self.filter_by_year_connection.send_message(
                routing_key="joiner_by_ratings_movies_queue",
                msg_body=msg.encode_to_str()
            )
        elif query_number == QueryNumber.QUERY_4:
            self.filter_by_year_connection.send_message(
                routing_key="joiner_by_credits_movies_queue",
                msg_body=msg.encode_to_str()
            )
