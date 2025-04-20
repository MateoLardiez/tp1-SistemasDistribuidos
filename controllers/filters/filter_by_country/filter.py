import pika
import logging
import csv
import io
from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.defines import QueryNumber
from common.middleware_connection_handler import RabbitMQConnectionHandler

PROD_COUNTRIES = 5
ID = "id"

class FilterByCountry:
    countries: list
    data: object

    def __init__(self):
        self.data = ""
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
#        self.connection_handler = RabbitMQConnectionHandler(producer_exchange_name="movies", ) # Hay que pasarle un routing key, y el exchange a donde se va a enviar.

        self.channel = self.connection.channel()
        self.countries = ["Spain"] 

    def start(self):
        logging.info("action: start | result: success | code: filter_by_country")
        self.channel.exchange_declare(exchange='country_filter', exchange_type='direct')
        

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

        ## 

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
        
        # Join all filtered lines into a single CSV string
        if filtered_lines:
            result_csv = '\n'.join([','.join(line) for line in filtered_lines])

        # 
        msg = MiddlewareMessage(
                query_number=1,
                client_id=1,
                type=MiddlewareMessageType.MOVIES_BATCH,
                payload=result_csv
            )
            # 436932,Jawbone,['Action', 'Drama'],2017-05-12,A former youth boxing champion, Jimmy McCabe is a man in search of hope but looking in all the wrong places. When he hits rock bottom he turns to his childhood boxing club and the only family he has left: gym owner Bill corner man Eddie and promoter Joe. Back in training, years after anyone thought he was a contender, he risks his life, as he tries to stand tall and regain his place in the world.,['United Kingdom'],['English'],0,0.0

        # Send all filtered results in a single message
        self.channel.basic_publish(exchange='country_filter', routing_key="filter_by_country_result", body=msg.encode_to_str())
        logging.info(f"action: send_filtered_batch | result: success | count: {len(filtered_lines)}")
        logging.info(f"FILTERED BATCH SENT: {len(filtered_lines)} movies matched filter criteria")
