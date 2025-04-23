import logging
from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.defines import QueryNumber
from common.middleware_connection_handler import RabbitMQConnectionHandler

PROD_COUNTRIES = 5
BUDGET = 7

class FilterByCountryInvesment:
    countries: list
    data: object

    def __init__(self):
        self.filter_by_country_connection = RabbitMQConnectionHandler(
            producer_exchange_name="filter_by_country_invesment_exchange",
            producer_queues_to_bind={"filter_by_country_invesment_queue": ["filter_by_country_invesment_queue"]},
            consumer_exchange_name="movies_preprocessor_exchange",
            consumer_queues_to_recv_from=["cleaned_movies_queue_country_invesment"],
        )
        # Configurar el callback para la cola específica
        self.filter_by_country_connection.set_message_consumer_callback("cleaned_movies_queue_country_invesment", self.callback)
        self.countries = ["Spain"]

    def start(self):
        logging.info("action: start | result: success | code: filter_by_country")
        self.filter_by_country_connection.start_consuming()
    
    def callback(self, ch, method, properties, body):
        data = MiddlewareMessage.decode_from_bytes(body)
        if data.type != MiddlewareMessageType.EOF_MOVIES:
            lines = data.get_batch_iter_from_payload()
            self.handler_filter(lines)
        else:
            logging.info(f"END OF FILE INVESMENT")
            msg = MiddlewareMessage(
                    query_number=data.query_number,
                    client_id=data.client_id,
                    type=MiddlewareMessageType.EOF_MOVIES,
                    payload=""
                )
            self.filter_by_country_connection.send_message(
                routing_key="filter_by_country_invesment_queue",
                msg_body=msg.encode_to_str()
            )

    def filter_by_country_invesment(self, movie):
        raw_value = movie[PROD_COUNTRIES].strip()#<- es un string

        if raw_value == "[]" or not raw_value:
            return False

        countries_of_movie = raw_value.strip("[]").replace("'", "").split(", ")
        countries_of_movie = [c for c in countries_of_movie if c.strip()]

        if len(countries_of_movie) == 1:
            return True
        return False

    def handler_filter(self, lines):
        filtered_lines = []
        for line in lines:
            if self.filter_by_country_invesment(line):
                result_data = [line[PROD_COUNTRIES], line[BUDGET]]
                filtered_lines.append(result_data)

        # Join all filtered lines into a single CSV string
        # id,title,genres,release_date,overview,production_countries,spoken_languages,budget,revenue
        if filtered_lines:
            result_csv = MiddlewareMessage.write_csv_batch(filtered_lines)
            # logging.info(f"INVESMENT MOVIES: {len(filtered_lines)}")
            msg = MiddlewareMessage(
                    query_number=1,
                    client_id=1,
                    type=MiddlewareMessageType.MOVIES_BATCH,
                    payload=result_csv
                )
            
            self.filter_by_country_connection.send_message(
                routing_key="filter_by_country_invesment_queue",
                msg_body=msg.encode_to_str()
            )
