import ast
import logging
from common.defines import QueryNumber
from common.middleware_connection_handler import RabbitMQConnectionHandler
from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType

ID = "id"
COLUMNS = [
    "id", "title", "genres", "release_date", "overview",
    "production_countries", "spoken_languages", "budget", "revenue"
]
COLUMNS_MOVIES =[
    "adult","belongs_to_collection","budget","genres","homepage","id","imdb_id",
    "original_language","original_title","overview","popularity","poster_path",
    "production_companies","production_countries","release_date","revenue","runtime",
    "spoken_languages","status","tagline","title","video","vote_average","vote_count"
]


class MoviesPreprocessor:
    countries: list
    data: object

    def __init__(self):
        self.movies_preprocessor_connection = RabbitMQConnectionHandler(
            producer_exchange_name="movies_preprocessor_exchange",
            producer_queues_to_bind={
                "filter_by_country_queue": ["filter_by_country_queue"],
                "filter_by_country_invesment_queue": ["filter_by_country_invesment_queue"],
                "aggregator_nlp_queue": ["aggregator_nlp_queue"]
            },
            consumer_exchange_name="gateway_exchange",
            consumer_queues_to_recv_from=["movies_queue"]
        )        
        # Configurar el callback para la cola específica
        self.movies_preprocessor_connection.set_message_consumer_callback("movies_queue", self.callback)

    def start(self):
        logging.info("action: start | result: success | code: movies_preprocessor")
        self.movies_preprocessor_connection.start_consuming()
    
    def callback(self, ch, method, properties, body):
        data = MiddlewareMessage.decode_from_bytes(body)
        lines = data.get_batch_iter_from_payload()
        clean_lines = self.clean_csv(lines)
        # logging.info(f" LINEAS LIMPIASSSS -> {clean_lines}")
        msg = MiddlewareMessage(
            query_number=data.query_number,
            client_id=data.client_id,
            type=MiddlewareMessageType.MOVIES_BATCH,
            payload=clean_lines,
        )

        if data.query_number == QueryNumber.ALL_QUERYS:
            self.movies_preprocessor_connection.send_message(routing_key="filter_by_country_queue", msg_body=msg.encode_to_str())
            self.movies_preprocessor_connection.send_message(routing_key="filter_by_country_invesment_queue", msg_body=msg.encode_to_str())
            #     self.movies_preprocessor_connection.send_message(routing_key="aggregator_nlp_queue", msg_body=msg.encode_to_str())
        elif data.query_number == QueryNumber.QUERY_1:
            self.movies_preprocessor_connection.send_message(routing_key="filter_by_country_queue", msg_body=msg.encode_to_str())
        elif data.query_number == QueryNumber.QUERY_2:
            self.movies_preprocessor_connection.send_message(routing_key="filter_by_country_invesment_queue", msg_body=msg.encode_to_str())
        # elif data.query_number == QueryNumber.QUERY_5:
        #     self.movies_preprocessor_connection.send_message(routing_key="aggregator_nlp_queue", msg_body=msg.encode_to_str())

    def clean_csv(self, reader):
        col_indices = {col: i for i, col in enumerate(COLUMNS_MOVIES) if col in COLUMNS}

        result = []

        for row in reader:
            if len(row) != len(COLUMNS_MOVIES):
                continue  # omitir filas mal formateadas

            # Crear un diccionario con los valores de las columnas necesarias
            row_dict = {col: row[col_indices[col]] for col in col_indices}
            
            for key in ['genres', 'production_countries', 'spoken_languages']:
                row_dict[key] = self.dictionary_to_list(row_dict[key])

            # Agregar los valores en el orden definido en COLUMNS
            filtered_row = [row_dict.get(col, '') for col in COLUMNS]
            result.append(filtered_row)

        return MiddlewareMessage.write_csv_batch(result)

    def dictionary_to_list(self, dictionary_str):
        try:
            dictionary_list = ast.literal_eval(dictionary_str)  
            return [data['name'] for data in dictionary_list]  
        except (ValueError, SyntaxError):
            return [] 