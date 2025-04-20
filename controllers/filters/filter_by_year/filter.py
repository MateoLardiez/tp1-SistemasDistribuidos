import pika
import logging
import csv
import io


from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.defines import QueryNumber

YEAR = 3 # release_date position
ID = "id"

class FilterByYear:
    year: int
    data: object

    def __init__(self):
        self.year = 2000
        self.data = ""
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        self.channel = self.connection.channel()
            

    def start(self):
        logging.info("action: start | result: success | code: filter_by_year")
        self.channel.exchange_declare(exchange='year_filter', exchange_type='direct')

        self.channel.exchange_declare(exchange='country_filter', exchange_type='direct')
        result = self.channel.queue_declare(queue='')
        queue_name = result.method.queue
        self.channel.queue_bind(exchange='country_filter', queue=queue_name, routing_key='filter_by_country_result')
        self.channel.basic_consume(queue=queue_name, on_message_callback=self.callback, auto_ack=True)
        self.channel.start_consuming()

    def callback(self, ch, method, properties, body):
        # id,title,genres,release_date,overview,production_countries,spoken_languages,budget,revenue
        data = MiddlewareMessage.decode_from_bytes(body)
        # logging.info(f"action: receive_RabbitMqmessage | result: success | code: {method.routing_key}")
        logging.info(f"action: receive_RabbitMqmessage | result: success | DATA RICA: {data}")
        lines = data.get_batch_iter_from_payload()


        if data.query_number == QueryNumber.QUERY_1:
            self.handler_filter_query_1(lines)

        # # logging.info(f"action: receive_RabbitMqmessage | result: success | code: {method.routing_key}")
        # # logging.info(f"action: receive_RabbitMqmessage | result: success | DATA RICA: {data}")

        # lines = csv.reader(io.StringIO(data), delimiter=',', quotechar='"')
        # filtered_lines = []

        # for line in lines:
        #     logging.info(f"LINEA ---------------> {line}   --------FIN DE LINEA")
        #     if self.filter_by_year(line):
        #         logging.info(f"action: filter | result: success | Pelicula que cumple: {line}")
        #         filtered_lines.append(line)
        
        # if filtered_lines:
        #     # Join all filtered lines into a single CSV string
        #     result_csv = '\n'.join([','.join(line) for line in filtered_lines])
            
        #     # Send all filtered results in a single message
        #     self.channel.basic_publish(exchange='results', routing_key="filter_by_year_result", body=result_csv)
        #     logging.info(f"action: send_filtered_batch | result: success | count: {len(filtered_lines)}")
        #     logging.info(f"FILTERED BATCH SENT: {len(filtered_lines)} movies matched filter criteria")
        
    def filter_by_year(self, movie):
        if len(movie) <= 3 or not movie[YEAR]:
            return False
        
        year_of_movie = movie[YEAR]
        try:
            release_year = int(year_of_movie.split('-')[0])
            return (release_year >= 2000)
        except (IndexError, ValueError):
            logging.error(f"Invalid release date format for movie: {movie}")
            return False

    def handler_filter_query_1(self, lines):
        filtered_lines = []
        for line in lines:
            logging.info(f"LINEA ---------------> {line}   --------FIN DE LINEA")
            if self.filter_by_year(line):
                logging.info(f"action: filter | result: success | Pelicula que cumple: {line}")
                filtered_lines.append(line)

        if filtered_lines:
            # Join all filtered lines into a single CSV string
            result_csv = '\n'.join([','.join(line) for line in filtered_lines])
            
            msg = MiddlewareMessage(
                query_number=1,
                client_id=1,
                type=MiddlewareMessageType.MOVIES_BATCH,
                payload=result_csv
            )


            # Send all filtered results in a single message
            self.channel.basic_publish(exchange='year_filter', routing_key="filter_by_year_result", body=msg.encode_to_str())
            logging.info(f"action: send_filtered_batch | result: success | count: {len(filtered_lines)}")
            logging.info(f"FILTERED BATCH SENT: {len(filtered_lines)} movies matched filter criteria")

