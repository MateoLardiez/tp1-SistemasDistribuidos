import logging
from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.defines import QueryNumber
from common.middleware_connection_handler import RabbitMQConnectionHandler

PROD_COUNTRIES = 5
ID = "id"

class FilterByCountry:
    countries: list
    data: object

    def __init__(self):
        self.filter_by_country_connection = RabbitMQConnectionHandler(
            producer_exchange_name="filter_by_country_exchange",
            producer_queues_to_bind={"filter_by_country_queue": ["filter_by_country_queue"]},
            consumer_exchange_name="gateway_exchange",
            consumer_queues_to_recv_from=["movies_queue"]
        )
        self.filter_by_country_connection.set_message_consumer_callback("movies_queue", self.callback)
        self.countries = ["Spain"] 

    def start(self):
        logging.info("action: start | result: success | code: filter_by_country")
        self.filter_by_country_connection.start_consuming()
    
    def callback(self, ch, method, properties, body):
        data = MiddlewareMessage.decode_from_bytes(body)
        lines = data.get_batch_iter_from_payload()
        if data.query_number == QueryNumber.QUERY_1:
            self.handler_filter_query_1(lines)     

    def filter_by_country(self, movie):
        countries_of_movie = movie[PROD_COUNTRIES]#<- es un string
        countries_of_movie = countries_of_movie.strip("[]").replace("'", "").split(", ")
        has_countries = all(country in countries_of_movie for country in self.countries)
        return has_countries

    def handler_filter_query_1(self, lines):
        filtered_lines = []
        for line in lines:
            if self.filter_by_country(line):
                logging.info(f"action: filter | result: success | Pelicula que cumple: {line}")
                filtered_lines.append(line)
        
        # Join all filtered lines into a single CSV string
        if filtered_lines:
            result_csv = '\n'.join([','.join(line) for line in filtered_lines])
            msg = MiddlewareMessage(
                    query_number=1,
                    client_id=1,
                    type=MiddlewareMessageType.MOVIES_BATCH,
                    payload=result_csv
                )
            
            self.filter_by_country_connection.send_message(
                routing_key="filter_by_country_queue",
                msg_body=msg.encode_to_str()
            )
