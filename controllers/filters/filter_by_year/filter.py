import pika
import logging

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
        data = body.decode('utf-8')
        logging.info(f"action: receive_RabbitMqmessage | result: success | code: {method.routing_key}")
        logging.info(f"action: receive_RabbitMqmessage | result: success | DATA RICA: {data}")

        
    # def filter_by_year()

