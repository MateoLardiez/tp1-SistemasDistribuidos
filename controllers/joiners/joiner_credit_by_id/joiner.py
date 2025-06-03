import logging
import csv
import os

from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.defines import QueryNumber
from common.middleware_connection_handler import RabbitMQConnectionHandler

YEAR = 3  # release_date position
class JoinerByCreditId:
    year: int
    data: object

    def __init__(self, id_worker, number_sinkers):
        self.joiner_by_credit_id_connection = RabbitMQConnectionHandler(
            producer_exchange_name="joiner_by_credit_id_exchange",
            producer_queues_to_bind={
                **{f"average_credit_aggregated_{i}": [f"average_credit_aggregated_{i}"] for i in range(number_sinkers)}
            },
            consumer_exchange_name="filter_by_year_exchange",
            consumer_queues_to_recv_from=[f"joiner_by_credits_movies_queue_{id_worker}", f"joiner_credits_by_id_queue_{id_worker}"],
            secondary_consumer_exchange_name="credits_preprocessor_exchange",
        )
        
        # Diccionario para almacenar el estado por cliente
        self.client_state = {}  # {client_id: {"movies_eof": bool, "credits_eof": bool}}     
        self.number_sinkers = number_sinkers   
        # Configurar callbacks para ambas colas
        self.joiner_by_credit_id_connection.set_message_consumer_callback(f"joiner_by_credits_movies_queue_{id_worker}", self.movies_callback)
        self.joiner_by_credit_id_connection.set_message_consumer_callback(f"joiner_credits_by_id_queue_{id_worker}", self.credits_callback)

    def start(self):
        logging.info("action: start | result: success | code: joiner_credit_by_id")
        self.joiner_by_credit_id_connection.start_consuming()
        
    def create_client_state(self, client_id):
        """Obtiene o crea el estado del cliente en el diccionario"""
        if client_id not in self.client_state:
            self.client_state[client_id] = {
                "movies_eof": False,
                "credits_eof": False,
                "movies_per_actor": {},
            }

                       
    def movies_callback(self, ch, method, properties, body):
        """Callback para procesar mensajes de la cola de movies"""
        data = MiddlewareMessage.decode_from_bytes(body)
        client_id = data.client_id
        
        if client_id not in self.client_state:
            self.create_client_state(client_id)
        
        if data.type != MiddlewareMessageType.EOF_MOVIES:
            lines = list(data.get_batch_iter_from_payload())
            self.save_data(client_id, lines, "movies")
        else:
            # Recibimos EOF de movies para este cliente
            self.client_state[client_id]["movies_eof"] = True
            self.loading_data(client_id, data.query_number)

    def credits_callback(self, ch, method, properties, body):
        """Callback para procesar mensajes de la cola de credits"""
        data = MiddlewareMessage.decode_from_bytes(body)
        client_id = data.client_id
        
        if client_id not in self.client_state:
            self.create_client_state(client_id)
         
        if data.type != MiddlewareMessageType.EOF_CREDITS:
            lines = list(data.get_batch_iter_from_payload())
            if not self.client_state[client_id]["movies_eof"]:    
                self.save_data(client_id, lines, "credits")
            else:
                self.process_credits(lines, client_id)
        else:
            # Recibimos EOF de credits para este cliente
            self.client_state[client_id]["credits_eof"] = True
            # Depuración: Mostrar estado actual
            self.send_results(client_id, data.query_number) 


    def process_credits(self, lines, client_id):
        """Process the credit data for a client"""
        movies_per_actor = self.client_state[client_id]["movies_per_actor"]
        for line in lines:
            # Assuming line is a list with the structure [credit_id, actor_names]
            movie_id = line[0]  # credit_id
            if movie_id in movies_per_actor:
                actor_names = line[1].strip("[]").replace("'", "").split(", ")  # Clean and split actor names
                movies_per_actor[movie_id] += actor_names
                            
            #logging.info(f"Processing credit data for client {client_id}: {line}")

        self.client_state[client_id]["movies_per_actor"] = movies_per_actor

    def send_results(self, client_id, query_number):
        """Send the results to the appropriate sinker"""
        
        # Enviar resultados procesados
        
        joined_data = self.client_state[client_id]["movies_per_actor"]
        
        logging.info(f"DATA TO SEND: {joined_data}")
        
        movies_per_actor = {}
        for _, actors in joined_data.items():
            for actor in actors:
                if actor not in movies_per_actor:
                    movies_per_actor[actor] = 0
                movies_per_actor[actor] += 1
        
        result = []
        [result.append([actor, count]) for actor, count in movies_per_actor.items()]

        result_csv = MiddlewareMessage.write_csv_batch(result) # TODO: Enviar en batches
        msg = MiddlewareMessage(
            query_number=query_number,
            client_id=client_id,
            type=MiddlewareMessageType.MOVIES_BATCH,
            seq_number=1,
            payload=result_csv
        )
        sinker_id = client_id % self.number_sinkers
        self.joiner_by_credit_id_connection.send_message(
            routing_key=f"average_credit_aggregated_{sinker_id}",
            msg_body=msg.encode_to_str()
        )
    
        msg_eof = MiddlewareMessage(
            query_number=query_number,
            client_id=client_id,
            type=MiddlewareMessageType.EOF_JOINER,
            seq_number=2,
            payload="" 
        )
        self.joiner_by_credit_id_connection.send_message(
            routing_key=f"average_credit_aggregated_{sinker_id}",
            msg_body=msg_eof.encode_to_str()
        )

        # # Limpiar los archivos temporales
        self.clean_temp_files(client_id)
        
        # # Eliminar el estado del cliente del diccionario
        del self.client_state[client_id]

    def loading_data(self, client_id, query_number):
        """Verifica si se han recibido ambos EOFs para un cliente y procesa los datos"""
        # Verificar que el cliente exista en el estado
        if client_id not in self.client_state:
            logging.warning(f"Cliente {client_id} no encontrado en el diccionario de estado")
            return
        
        # # Procesar los datos de movies y credits para este cliente
        movies_filename = f"movies-client-{client_id}"
        credits_filename = f"credits-client-{client_id}"
        
        joined_data = self.join_data(movies_filename, credits_filename)
        
        self.client_state[client_id]["movies_per_actor"] = joined_data

        
                
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
                if movie_id in credits:
                    actors = credits[movie_id]
                    actors_list = actors.strip("[]").replace("'", "").split(", ") # separo los actores por comas
                    # for actor in actors_list:
                    if movie_id not in movies_with_actors:
                        movies_with_actors[movie_id] = []   
                    movies_with_actors[movie_id] += actors_list # actores y cantidad de apariciones

        # result = []
        # #[result.append([actor, len(movies)]) for actor, movies in actors_with_movies.items()]
        # result = {actor: len(movies) for actor, movies in actors_with_movies.items()}
        logging.info(f"JOINED DATA: {movies_with_actors}")
        return movies_with_actors
            
    def clean_temp_files(self, client_id):
        """Elimina los archivos temporales creados para un cliente"""
        files_to_remove = [
            f"movies-client-{client_id}",
            f"credits-client-{client_id}"
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
        with open (file_name, 'r') as file:
            reader = csv.reader(file, quoting=csv.QUOTE_MINIMAL)
            for row in reader:
                yield row
