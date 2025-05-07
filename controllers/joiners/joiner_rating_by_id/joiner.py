import logging
import csv
import os

from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.defines import QueryNumber
from common.middleware_connection_handler import RabbitMQConnectionHandler

YEAR = 3  # release_date position
class JoinerByRatingId:
    year: int
    data: object

    def __init__(self, id_worker, number_sinkers):
        self.joiner_by_rating_id_connection = RabbitMQConnectionHandler(
            producer_exchange_name="joiner_by_rating_id_exchange",
            producer_queues_to_bind={
                **{f"average_rating_aggregated_{i}": [f"average_rating_aggregated_{i}"] for i in range(number_sinkers)},
            },
            consumer_exchange_name="filter_by_year_exchange",
            consumer_queues_to_recv_from=[f"joiner_by_ratings_movies_queue_{id_worker}", f"joiner_ratings_by_id_queue_{id_worker}"],
            secondary_consumer_exchange_name="ratings_preprocessor_exchange",
        )
        
        # Diccionario para almacenar el estado por cliente
        self.client_state = {}  # {client_id: {"movies_eof": bool, "ratings_eof": bool}}
        self.number_sinkers = number_sinkers
        # Configurar callbacks para ambas colas
        self.joiner_by_rating_id_connection.set_message_consumer_callback(f"joiner_by_ratings_movies_queue_{id_worker}", self.movies_callback)
        self.joiner_by_rating_id_connection.set_message_consumer_callback(f"joiner_ratings_by_id_queue_{id_worker}", self.ratings_callback)


    def start(self):
        logging.info("action: start | result: success | code: joiner_rating_by_id")
        self.joiner_by_rating_id_connection.start_consuming()
        
    def create_client_state(self, client_id):
        """Obtiene o crea el estado del cliente en el diccionario"""
        if client_id not in self.client_state:
            self.client_state[client_id] = {
                "movies_eof": False,
                "ratings_eof": False,
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
            # logging.info(f"EOF MOVIESSSSSSSSSSSSSS --------- ??? action: EOF | source: movies | client: {client_id}")
            self.client_state[client_id]["movies_eof"] = True
            # Depuración: Mostrar estado actual
            # logging.info(f"Estado cliente {client_id} después de EOF movies: movies_eof={self.client_state[client_id]['movies_eof']}, ratings_eof={self.client_state[client_id]['ratings_eof']}")
            self.check_and_process(client_id, data.query_number)

    def ratings_callback(self, ch, method, properties, body):
        """Callback para procesar mensajes de la cola de ratings"""
        data = MiddlewareMessage.decode_from_bytes(body)
        client_id = data.client_id
        
        if client_id not in self.client_state:
            self.create_client_state(client_id)
         
        if data.type != MiddlewareMessageType.EOF_RATINGS:
            lines = list(data.get_batch_iter_from_payload())
            self.save_data(client_id, lines, "ratings")
        else:
            # Recibimos EOF de ratings para este cliente
            self.client_state[client_id]["ratings_eof"] = True
            # Depuración: Mostrar estado actual
            self.check_and_process(client_id, data.query_number)

    def check_and_process(self, client_id, query_number):
        """Verifica si se han recibido ambos EOFs para un cliente y procesa los datos"""
        # Verificar que el cliente exista en el estado
        if client_id not in self.client_state:
            logging.warning(f"Cliente {client_id} no encontrado en el diccionario de estado")
            return
            
        movies_eof = self.client_state[client_id]["movies_eof"] 
        ratings_eof = self.client_state[client_id]["ratings_eof"]
        
        if movies_eof and ratings_eof:
            # # Procesar los datos de movies y ratings para este cliente
            movies_filename = f"movies-client-{client_id}"
            ratings_filename = f"ratings-client-{client_id}"
            
            joined_data = self.join_data(movies_filename, ratings_filename)
            
            # Enviar resultados procesados
            # if joined_data:
            result_csv = MiddlewareMessage.write_csv_batch(joined_data) # TODO: Enviar en batches
            msg = MiddlewareMessage(
                query_number=query_number,
                client_id=client_id,
                seq_number=1,
                type=MiddlewareMessageType.MOVIES_BATCH,
                payload=result_csv
            )
            sinker_id = client_id % self.number_sinkers
            self.joiner_by_rating_id_connection.send_message(
                routing_key=f"average_rating_aggregated_{sinker_id}",
                msg_body=msg.encode_to_str()
            )
        
            msg_eof = MiddlewareMessage(
                query_number=query_number,
                client_id=client_id,
                seq_number=2,
                type=MiddlewareMessageType.EOF_JOINER
            )
            self.joiner_by_rating_id_connection.send_message(
                routing_key=f"average_rating_aggregated_{sinker_id}",
                msg_body=msg_eof.encode_to_str()
            )
                
            # # Limpiar los archivos temporales
            self.clean_temp_files(client_id)
            
            # # Eliminar el estado del cliente del diccionario
            del self.client_state[client_id]
            
            # logging.info(f"action: process_joined_data | client: {client_id} | result: completed")
    
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

        for movies in self.read_data(movies_file):
            movies_id = movies[0]
            if movies_id in ratings:
                # Calculo el promedio
                ratings_accumulator = ratings[movies_id]["ratings_accumulator"]
                ratings_amount = ratings[movies_id]["ratings_amount"]
                average_rating = ratings_accumulator / ratings_amount
                # Agrego la info al resultado
                joined_results.append([movies[1], average_rating])
        
        return joined_results
            
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
        with open (file_name, 'r') as file:
            reader = csv.reader(file, quoting=csv.QUOTE_MINIMAL)
            for row in reader:
                yield row
