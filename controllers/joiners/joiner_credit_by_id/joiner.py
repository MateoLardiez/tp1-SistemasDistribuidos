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

    def __init__(self):
        self.joiner_by_credit_id_connection = RabbitMQConnectionHandler(
            producer_exchange_name="joiner_by_credit_id_exchange",
            producer_queues_to_bind={
                "average_credit_aggregated": ["average_credit_aggregated"],
            },
            consumer_exchange_name="filter_by_year_exchange",
            consumer_queues_to_recv_from=["joiner_by_credits_movies_queue", "joiner_credits_by_id_queue"],
            secondary_consumer_exchange_name="credits_preprocessor_exchange",
        )
        
        # Diccionario para almacenar el estado por cliente
        self.client_state = {}  # {client_id: {"movies_eof": bool, "credits_eof": bool}}        
        # Configurar callbacks para ambas colas
        self.joiner_by_credit_id_connection.set_message_consumer_callback("joiner_by_credits_movies_queue", self.movies_callback)
        self.joiner_by_credit_id_connection.set_message_consumer_callback("joiner_credits_by_id_queue", self.credits_callback)


    def start(self):
        logging.info("action: start | result: success | code: joiner_credit_by_id")
        self.joiner_by_credit_id_connection.start_consuming()
        
    def create_client_state(self, client_id):
        """Obtiene o crea el estado del cliente en el diccionario"""
        if client_id not in self.client_state:
            logging.info(f"Creando nuevo estado para cliente: {client_id}")
            self.client_state[client_id] = {
                "movies_eof": False,
                "credits_eof": False,
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
            self.check_and_process(client_id, data.query_number)

    def credits_callback(self, ch, method, properties, body):
        """Callback para procesar mensajes de la cola de credits"""
        data = MiddlewareMessage.decode_from_bytes(body)
        client_id = data.client_id
        
        if client_id not in self.client_state:
            self.create_client_state(client_id)
         
        if data.type != MiddlewareMessageType.EOF_CREDITS:
            lines = list(data.get_batch_iter_from_payload())
            self.save_data(client_id, lines, "credits")
        else:
            # Recibimos EOF de credits para este cliente
            self.client_state[client_id]["credits_eof"] = True
            # Depuración: Mostrar estado actual
            self.check_and_process(client_id, data.query_number)

    def check_and_process(self, client_id, query_number):
        """Verifica si se han recibido ambos EOFs para un cliente y procesa los datos"""
        # Verificar que el cliente exista en el estado
        if client_id not in self.client_state:
            logging.warning(f"Cliente {client_id} no encontrado en el diccionario de estado")
            return
            
        movies_eof = self.client_state[client_id]["movies_eof"] 
        credits_eof = self.client_state[client_id]["credits_eof"]
        
        # logging.info(f"check_and_process para cliente {client_id}: movies_eof={movies_eof}, credits_eof={credits_eof}")
        
        if movies_eof and credits_eof:
            # # Procesar los datos de movies y credits para este cliente
            movies_filename = f"movies-client-{client_id}"
            credits_filename = f"credits-client-{client_id}"
            
            joined_data = self.join_data(movies_filename, credits_filename)
            
            # Enviar resultados procesados
            # if joined_data:
            result_csv = MiddlewareMessage.write_csv_batch(joined_data) # TODO: Enviar en batches
            msg = MiddlewareMessage(
                query_number=query_number,
                client_id=client_id,
                type=MiddlewareMessageType.MOVIES_BATCH,
                payload=result_csv
            )
            self.joiner_by_credit_id_connection.send_message(
                routing_key="average_credit_aggregated",
                msg_body=msg.encode_to_str()
            )
        
            msg_eof = MiddlewareMessage(
                query_number=query_number,
                client_id=client_id,
                type=MiddlewareMessageType.EOF_JOINER
            )
            self.joiner_by_credit_id_connection.send_message(
                routing_key="average_credit_aggregated",
                msg_body=msg_eof.encode_to_str()
            )
                
    def join_data(self, movies_file, credits_file):
        actors_with_movies = {}
        # leo el archivo de credits
        credits = {} # diccionario de clave:valor -> id_pelicula: actores
        for credit in self.read_data(credits_file):
            credit_id = credit[0]
            actor_names = credit[1]           
            if credit_id not in credits:
                credits[credit_id] = ""
            credits[credit_id] = actor_names

        for movie in self.read_data(movies_file):
            movie_id = movie[0]

            if movie_id in credits:
                actors = credits[movie_id]
                actors_list = actors.strip("[]").replace("'", "").split(", ") # separo los actores por comas
                # logging.info(f"PELICUlA: {movie_id} | ACTORES: {actors}")
                for actor in actors_list:
                    if actor not in actors_with_movies:
                        actors_with_movies[actor] = []       
                    actors_with_movies[actor].append(movie_id) # actores y cantidad de apariciones
        
        result = []
        for actor, movies in actors_with_movies.items():
            result.append([actor, movies])
        return result
            
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
