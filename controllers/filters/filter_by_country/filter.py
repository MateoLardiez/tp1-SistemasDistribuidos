import pika
import logging

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
        self.channel.exchange_declare(exchange='movies', exchange_type='direct')
        result = self.channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        self.channel.queue_bind(exchange='movies', queue=queue_name, routing_key='filter_by_country')
        self.channel.basic_consume(queue=queue_name, on_message_callback=self.callback, auto_ack=True)
        self.channel.start_consuming()

    def callback(self, ch, method, properties, body):
        # id,title,genres,release_date,overview,production_countries,spoken_languages,budget,revenue
        data = body.decode('utf-8')
        # logging.info(f"action: receive_RabbitMqmessage | result: success | code: {method.routing_key}")
        # logging.info(f"action: receive_RabbitMqmessage | result: success | DATA RICA: {data}")

        # Filter the data
        line = data.split("\\")
        #logging.info(f"")
        # logging.info(f"Received data parsed: {line}, with len {len(line)}")
        if self.filter_by_country(line):
            logging.info(f"action: filter | result: success | Pelicula que cumple: {line}")


        # self.channel.basic_publish(exchange='results', routing_key="gateway_filter_country", body=line)

    def filter_by_country(self, movie):
        countries_of_movie = movie[PROD_COUNTRIES]#<- es un string
        countries_of_movie = countries_of_movie.strip("[]").replace("'", "").split(", ")
        has_countries = all(country in countries_of_movie for country in self.countries)
        # "['Germany', 'France', 'Hungary']"
        return has_countries
