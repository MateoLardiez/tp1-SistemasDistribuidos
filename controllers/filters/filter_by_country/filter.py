import logging
from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.defines import QueryNumber
from common.middleware_connection_handler import RabbitMQConnectionHandler

PROD_COUNTRIES = 5
ID = 0
TITLE = 1
GENRES = 2
YEAR = 3


class FilterByCountry:
    countries: list
    data: object

    def __init__(self):
        self.filter_by_country_connection = RabbitMQConnectionHandler(
            producer_exchange_name="filter_by_country_exchange",
            producer_queues_to_bind={"country_queue": ["country_queue"]},
            consumer_exchange_name="movies_preprocessor_exchange",
            consumer_queues_to_recv_from=["cleaned_movies_queue_country"]
        )        
        # Configurar el callback para la cola específica
        self.filter_by_country_connection.set_message_consumer_callback("cleaned_movies_queue_country", self.callback)
        self.countries_query_1 = ["Argentina", "Spain"] 
        self.countries_query_3 = ["Argentina"] 
        self.countries_query_4 = ["Argentina"]

    def start(self):
        logging.info("action: start | result: success | code: filter_by_country")
        self.filter_by_country_connection.start_consuming()
    
    def callback(self, ch, method, properties, body):
        data = MiddlewareMessage.decode_from_bytes(body)
        if data.type != MiddlewareMessageType.EOF_MOVIES:
            lines = data.get_batch_iter_from_payload()
            if data.query_number == QueryNumber.ALL_QUERYS:
                self.handler_all_query(lines, data.client_id, data.query_number)
            elif data.query_number == QueryNumber.QUERY_1:
                self.handler_country_filter(lines, self.countries_query_1, data.client_id, data.query_number)     
            elif data.query_number == QueryNumber.QUERY_3:
                self.handler_country_filter(lines, self.countries_query_3, data.client_id, data.query_number)
            elif data.query_number == QueryNumber.QUERY_4:
                self.handler_country_filter(lines, self.countries_query_4, data.client_id, data.query_number)
        else:
            logging.info("Received EOF_MOVIES message, stopping consumption.")
            if data.query_number == QueryNumber.ALL_QUERYS:
                self.handler_eof_all_querys(data)
            else:
                msg = MiddlewareMessage(
                    query_number=data.query_number,
                    client_id=data.client_id,
                    type=MiddlewareMessageType.EOF_MOVIES,
                    payload=""
                )
                self.filter_by_country_connection.send_message(
                    routing_key="country_queue",
                    msg_body=msg.encode_to_str()
                )       
            
    def filter_by_country(self, movie, country_filter):
        countries_of_movie = movie[PROD_COUNTRIES]#<- es un string
        countries_of_movie = countries_of_movie.strip("[]").replace("'", "").split(", ")
        has_countries = all(country in countries_of_movie for country in country_filter)
        return has_countries

    def handler_all_query(self, lines, id_client, query_number):
        lines_to_filter = []
        for line in lines:
            lines_to_filter.append(line)

        self.handler_country_filter(lines_to_filter, self.countries_query_3, id_client, QueryNumber.QUERY_3)
        self.handler_country_filter(lines_to_filter, self.countries_query_1, id_client, QueryNumber.QUERY_1)
        self.handler_country_filter(lines_to_filter, self.countries_query_4, id_client, QueryNumber.QUERY_4)
        
    def handler_country_filter(self, lines, countries_filter, id_client, query_number):
        filtered_lines = []
        for line in lines:
            if self.filter_by_country(line, countries_filter):
                filtered_lines.append(line)
        
        # Join all filtered lines into a single CSV string
        if filtered_lines:
            # Q1: [title, genres, release_date]
            # Q3: [id, title, release_date]
            # Q4: [id, release_date]
            result_csv = None
            msg = None
            if query_number == QueryNumber.QUERY_1:
                query_result = []
                for line in filtered_lines:
                    query_result.append([line[1], line[2], line[3]])
                result_csv = MiddlewareMessage.write_csv_batch(query_result)
                msg = MiddlewareMessage(
                    query_number=query_number,
                    client_id=id_client,
                    type=MiddlewareMessageType.MOVIES_BATCH,
                    payload=result_csv
                )
            if query_number == QueryNumber.QUERY_3:
                query_result = []
                for line in filtered_lines:
                    query_result.append([line[0], line[1], line[3]])
                result_csv = MiddlewareMessage.write_csv_batch(query_result)
                msg = MiddlewareMessage(
                    query_number=query_number,
                    client_id=id_client,
                    type=MiddlewareMessageType.MOVIES_BATCH,
                    payload=result_csv
                )
            if query_number == QueryNumber.QUERY_4:
                query_result = []
                for line in filtered_lines:
                    query_result.append([line[0], line[3]])
                result_csv = MiddlewareMessage.write_csv_batch(query_result)
                msg = MiddlewareMessage(
                    query_number=query_number,
                    client_id=id_client,
                    type=MiddlewareMessageType.MOVIES_BATCH,
                    payload=result_csv
                )
            if result_csv:
                self.filter_by_country_connection.send_message(
                    routing_key="country_queue",
                    msg_body=msg.encode_to_str()
                )

    def handler_eof_all_querys(self, data):
        for query_number in [QueryNumber.QUERY_1, QueryNumber.QUERY_3, QueryNumber.QUERY_4]:
            msg = MiddlewareMessage(
                query_number=query_number,
                client_id=data.client_id,
                type=MiddlewareMessageType.EOF_MOVIES,
                payload=""
            )
            self.filter_by_country_connection.send_message(
                routing_key="country_queue",
                msg_body=msg.encode_to_str()
            )
        