import logging
from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.defines import QueryNumber
from common.middleware_connection_handler import RabbitMQConnectionHandler

PROD_COUNTRIES = 5

class FilterByCountry:
    countries: list
    data: object

    def __init__(self):
        self.filter_by_country_connection = RabbitMQConnectionHandler(
            producer_exchange_name="filter_by_country_exchange",
            producer_queues_to_bind={"filter_by_country_queue": ["filter_by_country_queue"]},
            consumer_exchange_name="movies_preprocessor_exchange",
            consumer_queues_to_recv_from=["filter_by_country_queue"]
        )        
        # Configurar el callback para la cola específica
        self.filter_by_country_connection.set_message_consumer_callback("filter_by_country_queue", self.callback)
        self.countries_query_1 = ["United States of America"] 
        self.countries_query_3 = ["United States of America"] 
        self.countries_query_4 = ["United States of America"] 

    def start(self):
        logging.info("action: start | result: success | code: filter_by_country")
        self.filter_by_country_connection.start_consuming()
    
    def callback(self, ch, method, properties, body):
        data = MiddlewareMessage.decode_from_bytes(body)
        lines = data.get_batch_iter_from_payload()
        if data.query_number == QueryNumber.ALL_QUERYS:
            self.handler_all_query(lines, data.client_id, data.query_number)
        elif data.query_number == QueryNumber.QUERY_1:
            self.handler_country_filter(lines, self.countries_query_1, data.client_id, data.query_number)     

    def filter_by_country(self, movie, country_filter):
        countries_of_movie = movie[PROD_COUNTRIES]#<- es un string
        countries_of_movie = countries_of_movie.strip("[]").replace("'", "").split(", ")
        has_countries = all(country in countries_of_movie for country in country_filter)
        return has_countries

    def handler_all_query(self, lines, id_client, query_number):
        self.handler_country_filter(lines, self.countries_query_1, id_client, query_number)
        self.handler_country_filter(lines, self.countries_query_3, id_client, query_number)
        self.handler_country_filter(lines, self.countries_query_4, id_client, query_number)
        
    def handler_country_filter(self, lines, countries_filter, id_client, query_number):
        filtered_lines = []
        for line in lines:
            if self.filter_by_country(line, countries_filter):
                logging.info(f"action: filter | result: success | Pelicula que cumple: {line}")
                filtered_lines.append(line)
        
        # Join all filtered lines into a single CSV string
        if filtered_lines:
            result_csv = MiddlewareMessage.write_csv_batch(filtered_lines)
            msg = MiddlewareMessage(
                    query_number=query_number,
                    client_id=id_client,
                    type=MiddlewareMessageType.MOVIES_BATCH,
                    payload=result_csv
                )
            
            self.filter_by_country_connection.send_message(
                routing_key="filter_by_country_queue",
                msg_body=msg.encode_to_str()
            )
