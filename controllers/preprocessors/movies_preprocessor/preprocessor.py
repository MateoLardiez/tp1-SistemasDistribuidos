import ast
import logging
from common.defines import QueryNumber
from common.middleware_connection_handler import RabbitMQConnectionHandler
from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.resilient_node import ResilientNode

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


class MoviesPreprocessor(ResilientNode):
    countries: list
    data: object

    def __init__(self, number_workers, worker_id, nlp_workers):
        super().__init__()  # Call parent constructor
        self.worker_id = worker_id
        self.number_workers = number_workers
        self.nlp_workers = nlp_workers
        self.rabbitmq_connection_handler = RabbitMQConnectionHandler(
            producer_exchange_name="movies_preprocessor_exchange",
            producer_queues_to_bind={
                #"cleaned_movies_queue_country": ["cleaned_movies_queue_country" ],
                **{f"cleaned_movies_queue_country_{i}": [f"cleaned_movies_queue_country_{i}"] for i in range(nlp_workers)},
                **{f"cleaned_movies_queue_nlp_{i}": [f"cleaned_movies_queue_nlp_{i}"] for i in range(nlp_workers)},
                **{f"cleaned_movies_queue_country_invesment_{i}": [f"cleaned_movies_queue_country_invesment_{i}"] for i in range(number_workers)},
            },
            consumer_exchange_name="gateway_exchange",
            consumer_queues_to_recv_from=[f"movies_queue_{self.worker_id}"],
        )
        # Configurar el callback para la cola específica
        self.rabbitmq_connection_handler.set_message_consumer_callback(f"movies_queue_{self.worker_id}", self.callback)
        self.clients_state = {}  # Diccionario para almacenar el estado local de los clientes
        self.controller_name = f"movies_preprocessor_{worker_id}"
        self.load_state()

    def start(self):
        logging.info("action: start | result: success | code: movies_preprocessor")
        try:
            self.rabbitmq_connection_handler.start_consuming()
        except Exception as e:
            logging.info("Consuming stopped")

    def callback(self, ch, method, properties, body):
        data = MiddlewareMessage.decode_from_bytes(body)
        if data.type == MiddlewareMessageType.ABORT:
            logging.warning(f"action: abort | result: success | code: movies_preprocessor | client_id: {data.client_id} | seq_number: {data.seq_number}")
            # Enviar un mensaje de abortar a todos los workers
            # msg = MiddlewareMessage(
            #     query_number=data.query_number,
            #     client_id=data.client_id,
            #     type=MiddlewareMessageType.ABORT,
            #     seq_number=self.clients_state[data.client_id]["last_seq_number"],
            #     payload="",
            #     controller_name=self.controller_name
            # )
            # # for id_worker in range(self.number_workers):
            # #     self.rabbitmq_connection_handler.send_message(routing_key=f"cleaned_movies_queue_country_{id_worker}", msg_body=msg.encode_to_str())
            # #     self.rabbitmq_connection_handler.send_message(routing_key=f"cleaned_movies_queue_country_invesment_{id_worker}", msg_body=msg.encode_to_str())
            # for nlp_id in range(self.nlp_workers):
            #     self.rabbitmq_connection_handler.send_message(routing_key=f"cleaned_movies_queue_nlp_{nlp_id}", msg_body=msg.encode_to_str())
            del self.clients_state[data.client_id]  # Eliminar el estado del cliente para este controlador
            self.save_state()
            return  


        if data.client_id not in self.clients_state:
            self.clients_state[data.client_id] = {
                "last_seq_number": 0 # Este es el último seq number que propagamos
            }
        if data.controller_name not in self.clients_state[data.client_id]:
            self.clients_state[data.client_id][data.controller_name] = data.seq_number
        elif data.seq_number <= self.clients_state[data.client_id][data.controller_name]:
            logging.warning(f"Duplicated Message {data.client_id} in {data.controller_name} with  seq_number{data.seq_number}. Ignoring.")
            return
        
        
        if data.type != MiddlewareMessageType.EOF_MOVIES:    
            lines = data.get_batch_iter_from_payload()
            clean_lines = self.clean_csv(lines)
            # logging.info(f" LINEAS LIMPIASSSS -> {clean_lines}")
            msg = MiddlewareMessage(
                query_number=data.query_number,
                client_id=data.client_id,
                type=MiddlewareMessageType.MOVIES_BATCH,
                payload=clean_lines,
                seq_number=self.clients_state[data.client_id]["last_seq_number"], 
                controller_name=self.controller_name
            )
                
            # Round robin para enviar a los workers
            id_worker = self.clients_state[data.client_id]["last_seq_number"] % self.number_workers
            nlp_id = self.clients_state[data.client_id]["last_seq_number"] % self.nlp_workers
            if data.query_number == QueryNumber.ALL_QUERYS:
                self.rabbitmq_connection_handler.send_message(routing_key=f"cleaned_movies_queue_country_{id_worker}", msg_body=msg.encode_to_str())
                self.rabbitmq_connection_handler.send_message(routing_key=f"cleaned_movies_queue_country_invesment_{id_worker}", msg_body=msg.encode_to_str())
                self.rabbitmq_connection_handler.send_message(routing_key=f"cleaned_movies_queue_nlp_{nlp_id}", msg_body=msg.encode_to_str())
            elif data.query_number == QueryNumber.QUERY_1 or data.query_number == QueryNumber.QUERY_3 or data.query_number == QueryNumber.QUERY_4:
                self.rabbitmq_connection_handler.send_message(routing_key=f"cleaned_movies_queue_country_{id_worker}", msg_body=msg.encode_to_str())
            elif data.query_number == QueryNumber.QUERY_2:
                self.rabbitmq_connection_handler.send_message(routing_key=f"cleaned_movies_queue_country_invesment_{id_worker}", msg_body=msg.encode_to_str())
            elif data.query_number == QueryNumber.QUERY_5:
                self.rabbitmq_connection_handler.send_message(routing_key=f"cleaned_movies_queue_nlp_{nlp_id}", msg_body=msg.encode_to_str())

            # Actualizar el estado local del cliente
            self.clients_state[data.client_id]["last_seq_number"] += 1
            self.clients_state[data.client_id][data.controller_name] = data.seq_number
        elif data.type == MiddlewareMessageType.ABORT:
            logging.warning(f"action: abort | result: success | code: movies_preprocessor | client_id: {data.client_id} | seq_number: {data.seq_number}")
            # Enviar un mensaje de abortar a todos los workers
            msg = MiddlewareMessage(
                query_number=data.query_number,
                client_id=data.client_id,
                type=MiddlewareMessageType.ABORT,
                seq_number=self.clients_state[data.client_id]["last_seq_number"],
                payload="",
                controller_name=self.controller_name
            )
            # for id_worker in range(self.number_workers):
            #     self.rabbitmq_connection_handler.send_message(routing_key=f"cleaned_movies_queue_country_{id_worker}", msg_body=msg.encode_to_str())
            #     self.rabbitmq_connection_handler.send_message(routing_key=f"cleaned_movies_queue_country_invesment_{id_worker}", msg_body=msg.encode_to_str())
            for nlp_id in range(self.nlp_workers):
                self.rabbitmq_connection_handler.send_message(routing_key=f"cleaned_movies_queue_nlp_{nlp_id}", msg_body=msg.encode_to_str())
        else:
            seq_number = self.clients_state[data.client_id]["last_seq_number"]
            msg = MiddlewareMessage(
                        query_number=data.query_number,
                        client_id=data.client_id,
                        type=MiddlewareMessageType.EOF_MOVIES,
                        seq_number=seq_number,
                        payload="",
                        controller_name=self.controller_name
                    )
            if data.query_number == QueryNumber.ALL_QUERYS:
                self.handler_oef_all_querys(msg)
            elif data.query_number == QueryNumber.QUERY_1 or data.query_number == QueryNumber.QUERY_3 or data.query_number == QueryNumber.QUERY_4:
                self.handler_oef_query_1_3_4(msg)
            elif data.query_number == QueryNumber.QUERY_2:
                self.handler_oef_query_2(msg)
            elif data.query_number == QueryNumber.QUERY_5:
                self.handler_oef_query_5(msg)
            
            # del self.clients_state[data.client_id]  # Eliminar el estado del cliente para este controlador
            
        self.save_state()

    def handler_oef_all_querys(self, msg):

        for nlp_id in range(self.nlp_workers):
            self.rabbitmq_connection_handler.send_message(routing_key=f"cleaned_movies_queue_nlp_{nlp_id}", msg_body=msg.encode_to_str())

        for id_worker in range(self.number_workers):
            self.rabbitmq_connection_handler.send_message(routing_key=f"cleaned_movies_queue_country_invesment_{id_worker}", msg_body=msg.encode_to_str())
            self.rabbitmq_connection_handler.send_message(routing_key=f"cleaned_movies_queue_country_{id_worker}", msg_body=msg.encode_to_str())
    
    def handler_oef_query_1_3_4(self, msg):
        for id_worker in range(self.number_workers):
            self.rabbitmq_connection_handler.send_message(routing_key=f"cleaned_movies_queue_country_{id_worker}", msg_body=msg.encode_to_str())

    def handler_oef_query_2(self, msg):
        for id_worker in range(self.number_workers):
            self.rabbitmq_connection_handler.send_message(routing_key=f"cleaned_movies_queue_country_invesment_{id_worker}", msg_body=msg.encode_to_str())
     
    def handler_oef_query_5(self, msg):
        for nlp_id in range(self.nlp_workers):
            # Enviar el mensaje EOF a la cola de NLP
            self.rabbitmq_connection_handler.send_message(routing_key=f"cleaned_movies_queue_nlp_{nlp_id}", msg_body=msg.encode_to_str())
    
    def clean_csv(self, reader):
        col_indices = {col: i for i, col in enumerate(COLUMNS_MOVIES) if col in COLUMNS}

        result = []

        for row in reader:
            if len(row) != len(COLUMNS_MOVIES):
                continue  # omitir filas mal formateadas

            # Crear un diccionario con los valores de las columnas necesarias
            row_dict = {col: row[col_indices[col]] for col in col_indices}
            
            # Filtrar filas con valores nulos
            if any(row_dict[col] in (None, '', 'null') for col in col_indices):
                continue

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