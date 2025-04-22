import logging
from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.defines import QueryNumber
from common.middleware_connection_handler import RabbitMQConnectionHandler

PROD_COUNTRIES = 5

class FilterByCountryInvesment:
    countries: list
    data: object

    def __init__(self):
        self.filter_by_country_connection = RabbitMQConnectionHandler(
            producer_exchange_name="filter_by_country_invesment_exchange",
            producer_queues_to_bind={"filter_by_country_invesment_queue": ["filter_by_country_invesment_queue"]},
            consumer_exchange_name="movies_preprocessor_exchange",
            consumer_queues_to_recv_from=["filter_by_country_invesment_queue"]
        )
        # Configurar el callback para la cola específica
        self.filter_by_country_connection.set_message_consumer_callback("filter_by_country_invesment_queue", self.callback)
        self.countries = ["Spain"]

    def start(self):
        logging.info("action: start | result: success | code: filter_by_country")
        self.filter_by_country_connection.start_consuming()
    
    def callback(self, ch, method, properties, body):
        data = MiddlewareMessage.decode_from_bytes(body)
        lines = data.get_batch_iter_from_payload()
        self.handler_filter(lines)

    def filter_by_country_invesment(self, movie):
        countries_of_movie = movie[PROD_COUNTRIES]#<- es un string
        countries_of_movie = countries_of_movie.strip("[]").replace("'", "").split(", ")
        if len(countries_of_movie) == 1:
            logging.info(f"Cumple con el filtro: {movie}")
            return True
        return False

    def handler_filter(self, lines):
        filtered_lines = []
        for line in lines:
            if self.filter_by_country_invesment(line):
                filtered_lines.append(line)

        logging.info(f"RESULTTT INVESTMENT {filtered_lines}")

        # Join all filtered lines into a single CSV string
        # if filtered_lines:
        #     result_csv = '\n'.join([','.join(line) for line in filtered_lines])
        #     msg = MiddlewareMessage(
        #             query_number=1,
        #             client_id=1,
        #             type=MiddlewareMessageType.MOVIES_BATCH,
        #             payload=result_csv
        #         )
            
            # self.filter_by_country_connection.send_message(
            #     routing_key="filter_by_country_queue",
            #     msg_body=msg.encode_to_str()
            # )
