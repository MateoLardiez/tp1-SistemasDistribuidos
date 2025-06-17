import logging
import csv
import os

from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.defines import QueryNumber
from common.middleware_connection_handler import RabbitMQConnectionHandler
from common.resilient_node import ResilientNode
import ast

YEAR = 3  # release_date position
class JoinerByRatingId(ResilientNode):
    year: int
    data: object

    def __init__(self, id_worker, number_sinkers, number_workers):
        super().__init__()  # Call parent constructor
        self.rabbitmq_connection_handler = RabbitMQConnectionHandler(
            producer_exchange_name="joiner_by_rating_id_exchange",
            producer_queues_to_bind={
                **{f"average_rating_aggregated_{i}": [f"average_rating_aggregated_{i}"] for i in range(number_sinkers)},
            },
            consumer_exchange_name="filter_by_year_exchange",
            consumer_queues_to_recv_from=[f"joiner_by_ratings_movies_queue_{id_worker}", f"joiner_ratings_by_id_queue_{id_worker}"],
            secondary_consumer_exchange_name="ratings_preprocessor_exchange",
        )
        
        # Diccionario para almacenar el estado por cliente
        self.clients_state = {}  # {client_id: {"movies_eof": bool, "ratings_eof": bool}}
        self.number_sinkers = number_sinkers
        self.number_workers = number_workers  # Asumiendo que id_worker empieza en 0
        self.controller_name = f"joiner_rating_by_id_{id_worker}"
        # Configurar callbacks para ambas colas
        self.rabbitmq_connection_handler.set_message_consumer_callback(f"joiner_by_ratings_movies_queue_{id_worker}", self.movies_callback)
        self.rabbitmq_connection_handler.set_message_consumer_callback(f"joiner_ratings_by_id_queue_{id_worker}", self.ratings_callback)


    def start(self):
        logging.info("action: start | result: success | code: joiner_rating_by_id")
        try:
            self.rabbitmq_connection_handler.start_consuming()
        except Exception as e:
            logging.info("Consuming stopped")
        
    def create_clients_state(self, client_id):
        """Obtiene o crea el estado del cliente en el diccionario"""
        if client_id not in self.clients_state:
            self.clients_state[client_id] = {
                "movies_eof": 0,
                "ratings_eof": 0,
                "movies_with_ratings": {},
                "last_seq_number": 0,  # Este es el último seq number que propagamos
            }

    def movies_callback(self, ch, method, properties, body):
        """Callback para procesar mensajes de la cola de movies"""
        data = MiddlewareMessage.decode_from_bytes(body)
        client_id = data.client_id
        
        if client_id not in self.clients_state:
            self.create_clients_state(client_id)
        if data.controller_name not in self.clients_state[client_id]:
            self.clients_state[client_id][data.controller_name] = data.seq_number
        elif data.seq_number <= self.clients_state[client_id][data.controller_name]:
            logging.warning(f"Duplicated Message {client_id} in {data.controller_name} with seq_number {data.seq_number}. Ignoring.")
            return
        
        if data.type != MiddlewareMessageType.EOF_MOVIES:
            # Procesamos el mensaje de movies
            lines = list(data.get_batch_iter_from_payload())
            self.save_data(client_id, lines, "movies")
        else:
            # Recibimos EOF de movies para este cliente
            self.clients_state[client_id]["movies_eof"] += 1
            if self.clients_state[client_id]["movies_eof"] == self.number_workers:
                # Si hemos recibido EOF de movies de todos los workers, procedemos a cargar los datos
                self.loading_data(client_id)

    def ratings_callback(self, ch, method, properties, body):
        """Callback para procesar mensajes de la cola de ratings"""
        data = MiddlewareMessage.decode_from_bytes(body)
        client_id = data.client_id
        
        if client_id not in self.clients_state:
            self.create_clients_state(client_id)
        if data.controller_name not in self.clients_state[client_id]:
            self.clients_state[client_id][data.controller_name] = data.seq_number
        elif data.seq_number <= self.clients_state[client_id][data.controller_name]:
            logging.warning(f"Duplicated Message {client_id} in {data.controller_name} with seq_number {data.seq_number}. Ignoring.")
            return
         
        if data.type != MiddlewareMessageType.EOF_RATINGS:
            lines = list(data.get_batch_iter_from_payload())
            if self.clients_state[client_id]["movies_eof"] < self.number_workers:
                self.save_data(client_id, lines, "ratings")
            else:
                self.process_ratings(client_id, lines)
        else:
            # Recibimos EOF de ratings para este cliente
            self.clients_state[client_id]["ratings_eof"] += 1
            if self.clients_state[client_id]["ratings_eof"] == self.number_workers:
                # Depuración: Mostrar estado actual
                self.send_results(client_id, data.query_number)

    def loading_data(self, client_id):
        if client_id not in self.clients_state:
            logging.warning(f"Cliente {client_id} no encontrado en el diccionario de estado")
            return
        
        movies_filename = f"movies-client-{client_id}"
        ratings_filename = f"ratings-client-{client_id}"
            
        joined_data = self.join_data(movies_filename, ratings_filename)

        self.clients_state[client_id]["movies_with_ratings"] = joined_data


    def send_results(self, client_id, query_number):
        """Verifica si se han recibido ambos EOFs para un cliente y procesa los datos"""
        # Verificar que el cliente exista en el estado
        
        data = self.clients_state[client_id]["movies_with_ratings"]

        joined_data = {}
        for _, movie_info in data.items():
            if movie_info["ratings_amount"] > 0:
                if movie_info["title"] not in joined_data:
                    joined_data[movie_info["title"]] = 0.0
                joined_data[movie_info["title"]] = movie_info["ratings_accumulator"] / movie_info["ratings_amount"]
                
        result = []
        [result.append([title, rating]) for title, rating in joined_data.items()]

        result_csv = MiddlewareMessage.write_csv_batch(result) # TODO: Enviar en batches
        seq_number = self.clients_state[client_id]["last_seq_number"]
        msg = MiddlewareMessage(
            query_number=query_number,
            client_id=client_id,
            seq_number=seq_number,
            type=MiddlewareMessageType.MOVIES_BATCH,
            payload=result_csv,
            controller_name=self.controller_name
        )
        sinker_id = client_id % self.number_sinkers
        self.rabbitmq_connection_handler.send_message(
            routing_key=f"average_rating_aggregated_{sinker_id}",
            msg_body=msg.encode_to_str()
        )
    
        msg_eof = MiddlewareMessage(
            query_number=query_number,
            client_id=client_id,
            seq_number=seq_number + 1,
            type=MiddlewareMessageType.EOF_JOINER,
            payload="EOF",
            controller_name=self.controller_name
        )
        self.rabbitmq_connection_handler.send_message(
            routing_key=f"average_rating_aggregated_{sinker_id}",
            msg_body=msg_eof.encode_to_str()
        )
                
        # Limpiar los archivos temporales
        self.clean_temp_files(client_id)
        
        # Eliminar el estado del cliente del diccionario
        del self.clients_state[client_id]
        
        logging.info(f"action: process_joined_data | client: {client_id} | result: completed")
    
    def process_ratings(self, client_id, lines):
        movies_ratings = self.clients_state[client_id]["movies_with_ratings"]
        for line in lines:
            movie_id = line[0]
            rating = float(line[1])
            if movie_id in movies_ratings:
                movies_ratings[movie_id]["ratings_accumulator"] += rating
                movies_ratings[movie_id]["ratings_amount"] += 1

        self.clients_state[client_id]["movies_with_ratings"] = movies_ratings

    def join_data(self, movies_file, ratings_file):
        joined_results = []
        # leo el archivo de ratings
        ratings = {}
        for rating in self.read_data(ratings_file):
            if rating[0] not in ratings:
                ratings[rating[0]] = {
                    "ratings_accumulator": 0,
                    "ratings_amount": 0,
                }
            ratings[rating[0]]["ratings_accumulator"] += float(rating[1])
            ratings[rating[0]]["ratings_amount"] += 1

        movies_per_rating = {}
        for movies in self.read_data(movies_file):
            for movie in movies:
                movie_str = ast.literal_eval(movie)
                movie_id = movie_str[0]
                if movie_id not in movies_per_rating:
                    movies_per_rating[movie_id] = {
                        "title": "",
                        "ratings_accumulator": 0.0,
                        "ratings_amount": 0.0,
                    }
                movies_per_rating[movie_id]["title"] = movie_str[1]  # Agrego el año y el género
                             
        for movie_id, _ in movies_per_rating.items():      
            if movie_id in ratings:
                movies_per_rating[movie_id]["ratings_accumulator"] += ratings[movie_id]["ratings_accumulator"]
                movies_per_rating[movie_id]["ratings_amount"] += ratings[movie_id]["ratings_amount"]
 
        return movies_per_rating
            
    def clean_temp_files(self, client_id):
        """Elimina los archivos temporales creados para un cliente"""
        files_to_remove = [
            f"movies-client-{client_id}",
            f"ratings-client-{client_id}"
        ]
        
        for file in files_to_remove:
            try:
                if os.path.exists(file):
                    os.remove(file)
                    logging.info(f"action: clean_temp_files | file: {file} | result: removed")
            except Exception as e:
                logging.error(f"action: clean_temp_files | file: {file} | error: {str(e)}")
    
    def save_data(self, client_id, lines, data_type) -> None:
        filename = f"{data_type}-client-{client_id}"
        with open(filename, 'a+') as file:
            writer = csv.writer(file, quoting=csv.QUOTE_MINIMAL)
            for line in lines:
                writer.writerow(line)

    def read_data(self, file_name):
        try:
            with open(file_name, 'r') as file:
                reader = csv.reader(file, quoting=csv.QUOTE_MINIMAL)
                for row in reader:
                    yield row
        except (FileNotFoundError, IOError):
            return iter([])  # Retorna un iterador vacío si el archivo no existe o hay un error de I/O
