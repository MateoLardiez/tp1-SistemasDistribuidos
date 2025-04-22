import ast
import logging
from common.defines import QueryNumber
from common.middleware_connection_handler import RabbitMQConnectionHandler
from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType

ID = "id"
COLUMNS = ["id", "cast"]
COLUMNS_CREDITS =["cast", "crew", "id"]

class CreditsPreprocessor:
    countries: list
    data: object

    def __init__(self):
        self.credits_preprocessor_connection = RabbitMQConnectionHandler(
            producer_exchange_name="ratings_preprocessor_exchange",
            producer_queues_to_bind={
                "joiner_credits_by_id_queue": ["joiner_credits_by_id_queue"],
            },
            consumer_exchange_name="gateway_exchange",
            consumer_queues_to_recv_from=["credits_queue"]
        )        
        # Configurar el callback para la cola específica
        self.credits_preprocessor_connection.set_message_consumer_callback("credits_queue", self.callback)

    def start(self):
        logging.info("action: start | result: success | code: movies_preprocessor")
        self.credits_preprocessor_connection.start_consuming()
    
    def callback(self, ch, method, properties, body):
        data = MiddlewareMessage.decode_from_bytes(body)
        lines = data.get_batch_iter_from_payload()
        clean_lines = self.clean_csv(lines)
        msg = MiddlewareMessage(
            query_number=data.query_number,
            client_id=data.client_id,
            type=MiddlewareMessageType.MOVIES_BATCH,
            payload=clean_lines,
        )

        # if data.query_number == QueryNumber.ALL_QUERYS:
        #     self.ratings_preprocessor_connection.send_message(routing_key="filter_by_country_queue", msg_body=msg.encode_to_str())
        #     self.ratings_preprocessor_connection.send_message(routing_key="filter_by_country_invesment_queue", msg_body=msg.encode_to_str())
        #     #     self.movies_preprocessor_connection.send_message(routing_key="aggregator_nlp_queue", msg_body=msg.encode_to_str())
        # elif data.query_number == QueryNumber.QUERY_1:
        #     self.ratings_preprocessor_connection.send_message(routing_key="filter_by_country_queue", msg_body=msg.encode_to_str())
        # elif data.query_number == QueryNumber.QUERY_2:
        #     self.ratings_preprocessor_connection.send_message(routing_key="filter_by_country_invesment_queue", msg_body=msg.encode_to_str())
        # elif data.query_number == QueryNumber.QUERY_5:
        #     self.movies_preprocessor_connection.send_message(routing_key="aggregator_nlp_queue", msg_body=msg.encode_to_str())

    def clean_csv(self, reader):
        col_indices = {col: i for i, col in enumerate(COLUMNS_CREDITS) if col in COLUMNS}

        result = []

        for row in reader:
            if len(row) != len(COLUMNS_CREDITS):
                continue  # omitir filas mal formateadas

            # Crear un diccionario con los valores de las columnas necesarias
            row_dict = {col: row[col_indices[col]] for col in col_indices}
            
            for key in ['cast']:
                row_dict[key] = self.dictionary_to_list(row_dict[key])

            # Agregar los valores en el orden definido en COLUMNS
            filtered_row = [row_dict.get(col, '') for col in COLUMNS]

            logging.info(f" FILTRO -> {filtered_row}")
            result.append(filtered_row)

        return MiddlewareMessage.write_csv_batch(result)

    def dictionary_to_list(self, dictionary_str):
        try:
            dictionary_list = ast.literal_eval(dictionary_str)  
            return [data['name'] for data in dictionary_list]  
        except (ValueError, SyntaxError):
            return [] 