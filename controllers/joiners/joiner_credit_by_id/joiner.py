import logging

from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.defines import QueryNumber
from common.middleware_connection_handler import RabbitMQConnectionHandler
from common.resilient_node import ResilientNode
from common.file_manager import FileManager
import time

YEAR = 3  # release_date position
class JoinerByCreditId(ResilientNode):
    year: int
    data: object

    def __init__(self, id_worker, number_sinkers, number_workers):
        super().__init__()  # Call parent constructor
        self.rabbitmq_connection_handler = RabbitMQConnectionHandler(
            producer_exchange_name="joiner_by_credit_id_exchange",
            producer_queues_to_bind={
                **{f"average_credit_aggregated_{i}": [f"average_credit_aggregated_{i}"] for i in range(number_sinkers)}
            },
            consumer_exchange_name="filter_by_year_exchange",
            consumer_queues_to_recv_from=[f"joiner_by_credits_movies_queue_{id_worker}", f"joiner_credits_by_id_queue_{id_worker}"],
            secondary_consumer_exchange_name="credits_preprocessor_exchange",
        )
        
        # Diccionario para almacenar el estado por cliente
        self.clients_state = {}  # {client_id: {"movies_eof": bool, "credits_eof": bool}}     
        self.number_sinkers = number_sinkers
        self.number_workers = number_workers  # Asumiendo que id_worker empieza en 0   
        self.controller_name = f"joiner_by_credit_id_{id_worker}"
        # Configurar callbacks para ambas colas
        self.rabbitmq_connection_handler.set_message_consumer_callback(f"joiner_by_credits_movies_queue_{id_worker}", self.movies_callback)
        self.rabbitmq_connection_handler.set_message_consumer_callback(f"joiner_credits_by_id_queue_{id_worker}", self.credits_callback)
        self.load_state(self.check_files_state)  # Cargar el estado de los clientes desde el archivo

    def start(self):
        logging.info("action: start | result: success | code: joiner_credit_by_id")
        try:
            self.rabbitmq_connection_handler.start_consuming()
        except Exception as e:
            logging.error(f"Consuming stopped")

    def check_files_state(self):
        for client_id in self.clients_state:
            if self.check_file(client_id, "movies"):
                self.clients_state[client_id]["duplicated_batch"]["movies"] = True
            if self.check_file(client_id, "credits"):
                self.clients_state[client_id]["duplicated_batch"]["credits"] = True

    def create_clients_state(self, client_id):
        """Obtiene o crea el estado del cliente en el diccionario"""
        if client_id not in self.clients_state:
            self.clients_state[client_id] = {
                "movies_eof": 0,
                "credits_eof": 0,
                "last_seq_number": 0,  # Último seq_number procesado
                "movies_per_actor": {},
                "hash_file": {
                    "movies": None,  # Hash del archivo de movies
                    "credits": None
                },  # TODO: ÚHash deterministo del archivo actualizado
                "duplicated_batch": {
                    "movies": False,  # Indica si viene un batch de movies duplicado
                    "credits": False  # Indica si viene un batch de credits duplicado
                },  # Indica si se debe deduplicar el archivo
            }
        # Chequear archivos al iniciar la carga del estado

    def movies_callback(self, ch, method, properties, body):
        """Callback para procesar mensajes de la cola de movies"""
        data = MiddlewareMessage.decode_from_bytes(body)

        if data.type == MiddlewareMessageType.ABORT:
            logging.info(f"Received ABORT message from client {data.client_id}. Stopping processing.")
            self.handle_abort_message(data)
            return

        client_id = data.client_id      
        if client_id not in self.clients_state:
            self.create_clients_state(client_id)
            
        if data.controller_name not in self.clients_state[client_id]:
            self.clients_state[client_id][data.controller_name] = data.seq_number
        elif self.clients_state[client_id]["duplicated_batch"]["movies"]:
            logging.warning(f"Duplicated batch of movies for client {client_id}. Ignoring.")
            self.update_duplicate_state(client_id, "movies", data.controller_name, data.seq_number)
            return
        elif data.seq_number <= self.clients_state[client_id][data.controller_name]:
            logging.warning(f"Duplicated Message {client_id} in {data.controller_name} with seq_number {data.seq_number}. Ignoring.")    
            return
        
        if data.type != MiddlewareMessageType.EOF_MOVIES:
            lines = list(data.get_batch_iter_from_payload())
            filename = f".data/movies-client-{client_id}"
            self.clients_state[client_id]["hash_file"]["movies"] = self.save_data(filename, lines) # csv y .tmp
            self.clients_state[client_id][data.controller_name] = data.seq_number
        else:
            # Recibimos EOF de movies para este cliente
            self.clients_state[client_id]["movies_eof"] += 1
            if self.clients_state[client_id]["movies_eof"] == self.number_workers:
                self.loading_data(client_id)
        self.save_state()  # Guardar el estado de los clientes en el archivo
        
    def credits_callback(self, ch, method, properties, body):
        """Callback para procesar mensajes de la cola de credits"""
        data = MiddlewareMessage.decode_from_bytes(body)

        if data.type == MiddlewareMessageType.ABORT:
            logging.info(f"Received ABORT message from client {data.client_id}. Stopping processing.")
            self.handle_abort_message(data)
            return

        client_id = data.client_id
        if client_id not in self.clients_state:
            self.create_clients_state(client_id)

        if data.controller_name not in self.clients_state[client_id]:
            self.clients_state[client_id][data.controller_name] = data.seq_number
        elif self.clients_state[client_id]["duplicated_batch"]["credits"]:
            logging.warning(f"Duplicated batch of credits for client {client_id}. Ignoring.")
            self.update_duplicate_state(client_id, "credits", data.controller_name, data.seq_number)
            return
        elif data.seq_number <= self.clients_state[client_id][data.controller_name]:
            logging.warning(f"Duplicated Message {client_id} in {data.controller_name} with seq_number {data.seq_number}. Ignoring.")
            return
         
        if data.type != MiddlewareMessageType.EOF_CREDITS:
            lines = list(data.get_batch_iter_from_payload())
            if self.clients_state[client_id]["movies_eof"] < self.number_workers:
                filename = f".data/credits-client-{client_id}"   
                self.clients_state[client_id]["hash_file"]["credits"] = self.save_data(filename, lines)
            else:
                self.process_credits(lines, client_id)
            self.clients_state[client_id][data.controller_name] = data.seq_number
        else:
            # Recibimos EOF de credits para este cliente
            self.clients_state[client_id]["credits_eof"] += 1
            if self.clients_state[client_id]["credits_eof"] == self.number_workers:
                # Depuración: Mostrar estado actual
                self.send_results(client_id, data.query_number)
        self.save_state()  # Guardar el estado de los clientes en el archivo

    def handle_abort_message(self, data):
        """Maneja el mensaje de aborto recibido"""
        logging.info(f"Received ABORT message from client {data.client_id}. Stopping processing.")
        if data.client_id in self.clients_state:
            msg = MiddlewareMessage(
                query_number=data.query_number,
                client_id=data.client_id,
                type=MiddlewareMessageType.ABORT,
                seq_number=data.seq_number,
                payload="",
                controller_name=self.controller_name
            )
            id_sinker = data.client_id % self.number_sinkers
            # Enviar el mensaje de ABORT a todos los sinkers
            self.rabbitmq_connection_handler.send_message(
                routing_key=f"average_rating_aggregated_{id_sinker}",
                msg_body=msg.encode_to_str()
            )
            files_to_remove = [
                f".data/movies-client-{data.client_id}",
                f".data/credits-client-{data.client_id}",
            ]
            FileManager.clean_temp_files(files_to_remove)
            del self.clients_state[data.client_id]
            self.save_state()

    def process_credits(self, lines, client_id):
        """Process the credit data for a client"""
        movies_per_actor = self.clients_state[client_id]["movies_per_actor"]
        for line in lines:
            # Assuming line is a list with the structure [credit_id, actor_names]
            movie_id = line[0]  # credit_id
            if movie_id in movies_per_actor:
                actor_names = line[1].strip("[]").replace("'", "").split(", ")  # Clean and split actor names
                movies_per_actor[movie_id] += actor_names
                            
            #logging.info(f"Processing credit data for client {client_id}: {line}")

        self.clients_state[client_id]["movies_per_actor"] = movies_per_actor

    def send_results(self, client_id, query_number):
        """Send the results to the appropriate sinker"""
        
        # Enviar resultados procesados
        
        joined_data = self.clients_state[client_id]["movies_per_actor"]
        
        movies_per_actor = {}
        for _, actors in joined_data.items():
            for actor in actors:
                if actor not in movies_per_actor:
                    movies_per_actor[actor] = 0
                movies_per_actor[actor] += 1
        
        result = []
        [result.append([actor, count]) for actor, count in movies_per_actor.items()]

        result_csv = MiddlewareMessage.write_csv_batch(result) # TODO: Enviar en batches
        seq_number = self.clients_state[client_id]["last_seq_number"]
        msg = MiddlewareMessage(
            query_number=query_number,
            client_id=client_id,
            type=MiddlewareMessageType.MOVIES_BATCH,
            seq_number=seq_number,
            payload=result_csv,
            controller_name=self.controller_name
        )
        sinker_id = client_id % self.number_sinkers
        self.rabbitmq_connection_handler.send_message(
            routing_key=f"average_credit_aggregated_{sinker_id}",
            msg_body=msg.encode_to_str()
        )
    
        msg_eof = MiddlewareMessage(
            query_number=query_number,
            client_id=client_id,
            type=MiddlewareMessageType.EOF_JOINER,
            seq_number=seq_number + 1,
            payload="",
            controller_name=self.controller_name
        )
        self.rabbitmq_connection_handler.send_message(
            routing_key=f"average_credit_aggregated_{sinker_id}",
            msg_body=msg_eof.encode_to_str()
        )

        # Limpiar los archivos temporales
        files_to_remove = [
            f".data/movies-client-{client_id}",
            f".data/credits-client-{client_id}"
        ]
        FileManager.clean_temp_files(files_to_remove)
        
        # Eliminar el estado del cliente del diccionario
        del self.clients_state[client_id]

    def loading_data(self, client_id):
        """Verifica si se han recibido ambos EOFs para un cliente y procesa los datos"""
        # Verificar que el cliente exista en el estado
        if client_id not in self.clients_state:
            logging.warning(f"Cliente {client_id} no encontrado en el diccionario de estado")
            return
        
        # # Procesar los datos de movies y credits para este cliente
        movies_filename = f".data/movies-client-{client_id}"
        credits_filename = f".data/credits-client-{client_id}"

        joined_data = self.join_data(movies_filename, credits_filename)
        
        self.clients_state[client_id]["movies_per_actor"] = joined_data
                
    def join_data(self, movies_file, credits_file):     
        movies_with_actors = {}
        credits = {} # diccionario de clave:valor -> id_pelicula: actores
        for credit in self.read_data(credits_file):
            credit_id = credit[0]
            actor_names = credit[1]           
            if credit_id not in credits:
                credits[credit_id] = ""
            credits[credit_id] = actor_names
        
        for movies in self.read_data(movies_file):
            for movie in movies:
                movie_id = movie
                if movie_id not in movies_with_actors:
                    movies_with_actors[movie_id] = [] 
                    
        for movie_id, _ in movies_with_actors.items():
            if movie_id in credits:
                actors = credits[movie_id]
                actors_list = actors.strip("[]").replace("'", "").split(", ") # separo los actores por comas
                movies_with_actors[movie_id] += actors_list # actores y cantidad de apariciones

        return movies_with_actors

    def save_data(self, filename, lines) -> None:
        writer = FileManager(filename)
        return writer.save_data(filename, lines)

    def read_data(self, filename):
        reader = FileManager(filename)
        return reader.read()