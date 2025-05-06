import ast
import logging
from common.defines import QueryNumber
from common.middleware_connection_handler import RabbitMQConnectionHandler
from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType

ID = 0
COLUMNS = ["id", "cast"]
COLUMNS_CREDITS =["cast", "crew", "id"]

class CreditsPreprocessor:
    countries: list
    data: object

    def __init__(self, numberWorkers):
        self.credits_preprocessor_connection = RabbitMQConnectionHandler(
            producer_exchange_name="ratings_preprocessor_exchange",
            producer_queues_to_bind={
                **{f"joiner_credits_by_id_queue_{i}": [f"joiner_credits_by_id_queue_{i}"] for i in range(numberWorkers)},
                # "joiner_credits_by_id_queue": ["joiner_credits_by_id_queue"],
            },
            consumer_exchange_name="gateway_exchange",
            consumer_queues_to_recv_from=["credits_queue"]
        )        
        # Configurar el callback para la cola específica
        self.credits_preprocessor_connection.set_message_consumer_callback("credits_queue", self.callback)
        self.numberWorkers = numberWorkers

    def start(self):
        logging.info("action: start | result: success | code: credits_preprocessor")
        self.credits_preprocessor_connection.start_consuming()
    
    def callback(self, ch, method, properties, body):
        try:
            data = MiddlewareMessage.decode_from_bytes(body)
            if data.type != MiddlewareMessageType.EOF_CREDITS:
                lines = data.get_batch_iter_from_payload()
                clean_lines = self.clean_csv(lines)
                for sharding_id, data_shard in clean_lines.items():
                    data_csv = MiddlewareMessage.write_csv_batch(data_shard)
                    msg = MiddlewareMessage(
                        query_number=data.query_number,
                        client_id=data.client_id,
                        seq_number=data.seq_number,
                        type=MiddlewareMessageType.CREDITS_BATCH,
                        payload=data_csv,
                    )
                    self.credits_preprocessor_connection.send_message(
                        routing_key=f"joiner_credits_by_id_queue_{sharding_id}",
                        msg_body=msg.encode_to_str()
                    )
            else:
                for i in range(self.numberWorkers):
                    msg = MiddlewareMessage(
                        query_number=data.query_number,
                        client_id=data.client_id,
                        seq_number=data.seq_number,
                        type=MiddlewareMessageType.EOF_CREDITS,
                        payload=""
                    )
                    self.credits_preprocessor_connection.send_message(
                        routing_key=f"joiner_credits_by_id_queue_{i}",
                        msg_body=msg.encode_to_str()
                    )
        except Exception as e:
            logging.error(f"Error en el callback: {e}")

    def clean_csv(self, reader):
        col_indices = {col: i for i, col in enumerate(COLUMNS_CREDITS) if col in COLUMNS}

        result = {}

        for row in reader:
            if len(row) != len(COLUMNS_CREDITS):
                continue  # omitir filas mal formateadas

            # Crear un diccionario con los valores de las columnas necesarias
            row_dict = {col: row[col_indices[col]] for col in col_indices}            

            for key in ['cast']:
                row_dict[key] = self.dictionary_to_list(row_dict[key])

            # Agregar los valores en el orden definido en COLUMNS
            filtered_row = [row_dict.get(col, '') for col in COLUMNS]

            sharding_key = int(filtered_row[ID]) % self.numberWorkers
            if sharding_key not in result:
                result[sharding_key] = []
            if filtered_row[1]:
                result[sharding_key].append(filtered_row)
        
        return result

    def dictionary_to_list(self, dictionary_str):
        try:
            dictionary_list = ast.literal_eval(dictionary_str)  
            return [data['name'] for data in dictionary_list]  
        except (ValueError, SyntaxError):
            return [] 