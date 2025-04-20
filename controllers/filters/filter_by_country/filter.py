import pika
import logging
import csv
import io
from common.middleware_message_protocol import MiddlewareMessage
from common.defines import QueryNumber

PROD_COUNTRIES = 5
ID = "id"

class FilterByCountry:
    countries: list
    data: object

    def __init__(self):
        self.data = ""
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        self.channel = self.connection.channel()
        self.countries = ["Spain"] 

    def start(self):
        logging.info("action: start | result: success | code: filter_by_country")
        self.channel.exchange_declare(exchange='results', exchange_type='direct')
        

        self.channel.exchange_declare(exchange='movies', exchange_type='direct')
        result = self.channel.queue_declare(queue='')
        queue_name = result.method.queue
        self.channel.queue_bind(exchange='movies', queue=queue_name, routing_key='filter_by_country')
        self.channel.basic_consume(queue=queue_name, on_message_callback=self.callback, auto_ack=True)
        self.channel.start_consuming()
    
    def callback(self, ch, method, properties, body):
        # id,title,genres,release_date,overview,production_countries,spoken_languages,budget,revenue
        data = MiddlewareMessage.decode_from_bytes(body)
        # logging.info(f"action: receive_RabbitMqmessage | result: success | code: {method.routing_key}")
        # logging.info(f"action: receive_RabbitMqmessage | result: success | DATA RICA: {data}")
        lines = data.get_batch_iter_from_payload()
        if data.query_number == QueryNumber.QUERY_1:
            self.handler_filter_query_1(lines)

        

    def filter_by_country(self, movie):
        countries_of_movie = movie[PROD_COUNTRIES]#<- es un string
        countries_of_movie = countries_of_movie.strip("[]").replace("'", "").split(", ")
        has_countries = all(country in countries_of_movie for country in self.countries)
        # "['Germany', 'France', 'Hungary']"
        return has_countries

    def handler_filter_query_1(self, lines):
        filtered_lines = []
        for line in lines:
            logging.info(f"LINEA ---------------> {line}   --------FIN DE LINEA")
            if self.filter_by_country(line):
                logging.info(f"action: filter | result: success | Pelicula que cumple: {line}")
                filtered_lines.append(line)
        
        if filtered_lines:
            # Join all filtered lines into a single CSV string
            result_csv = '\n'.join([','.join(line) for line in filtered_lines])
            
            # Send all filtered results in a single message
            self.channel.basic_publish(exchange='results', routing_key="filter_by_country_result", body=result_csv)
            logging.info(f"action: send_filtered_batch | result: success | count: {len(filtered_lines)}")
            logging.info(f"FILTERED BATCH SENT: {len(filtered_lines)} movies matched filter criteria")
