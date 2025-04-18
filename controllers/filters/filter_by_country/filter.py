import pika
import logging

PROD_COUNTRIES = "production_countries"
ID = "id"

class FilterByCountry:
    countries: list
    data: object

    def __init__(self):
        self.countries = ""
        self.data = ""
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        self.channel = self.connection.channel()
            

    def start(self):
        logging.info("action: start | result: success | code: filter_by_country")
        self.channel.exchange_declare(exchange='movies', exchange_type='direct')
        result = self.channel.queue_declare(queue='', exclusive=True)
        queue_name = result.method.queue
        self.channel.queue_bind(exchange='movies', queue=queue_name, routing_key='filter_by_country')
        self.channel.basic_consume(queue=queue_name, on_message_callback=self.callback, auto_ack=True)
        self.channel.start_consuming()

    def callback(self, ch, method, properties, body):
        data = body.decode('utf-8')
        logging.info(f"action: receive_RabbitMqmessage | result: success | code: {method.routing_key}")
        logging.info(f"action: receive_RabbitMqmessage | result: success | DATA RICA: {data}")

        

    # def filter_by_country():
    #     movies = data.get_movies_from_payload()
    #     movies
    #     movies_filtered = dict()

    #     for movie in movies:
    #         countries_of_movie = movie[PROD_COUNTRIES]

    #         has_countries = all(c in countries_of_movie for country in self.countries)
    #         if has_countries:
    #             movies_filtered[movie[ID]] = movie



    #     return movies_filtered
