import pika
import logging
import csv
import io

ID = "id"

class FilterByYear:
    countries: list
    data: object

    def __init__(self):
        self.countries = ""
        self.data = ""
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        self.channel = self.connection.channel()
            

    def start(self):
        logging.info("action: start | result: success | code: filter_by_year")
        self.channel.exchange_declare(exchange='movies', exchange_type='direct')
        result = self.channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        self.channel.queue_bind(exchange='movies', queue=queue_name, routing_key='filter_by_year')
        self.channel.basic_consume(queue=queue_name, on_message_callback=self.callback, auto_ack=True)
        self.channel.start_consuming()

    def callback(self, ch, method, properties, body):
        # id,title,genres,release_date,overview,production_countries,spoken_languages,budget,revenue
        data = body.decode('utf-8')
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
        
    # def filter_by_year(self, movie):
    #     # Assuming release_date is in the format YYYY-MM-DD in position 3
    #     if len(movie) <= 3 or not movie[3]:
    #         return False
            
    #     try:
    #         release_year = int(movie[3].split('-')[0])
    #         # Filter movies from 2010 onwards
    #         return release_year >= 2010
    #     except (IndexError, ValueError):
    #         logging.error(f"Invalid release date format for movie: {movie}")
    #         return False

