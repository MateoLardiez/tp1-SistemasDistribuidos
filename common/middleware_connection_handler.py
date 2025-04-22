from typing import Callable
import pika

class RabbitMQConnectionHandler:
    def __init__(self, 
                 producer_exchange_name: str | None, 
                 producer_queues_to_bind: dict[str,list[str]] | None, 
                 consumer_exchange_name: str | None, 
                 consumer_queues_to_recv_from: list[str] | None,
                 secondary_consumer_exchange_name: str | None = None
                 ):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq', heartbeat=3600))
        self.channel = self.connection.channel()

        if producer_exchange_name is not None:
            self.__configure_producer_bindings(producer_exchange_name, producer_queues_to_bind)
        if consumer_exchange_name is not None:
            self.__configure_consumer_queues(consumer_exchange_name, consumer_queues_to_recv_from, secondary_consumer_exchange_name)
        self.channel.basic_qos(prefetch_count=1)
        self.channel.confirm_delivery()

    def __configure_consumer_queues(self, 
                              consumer_exchange_name: str, 
                              consumer_queues_to_recv_from: list[str],
                              secondary_consumer_exchange_name: str | None = None
                              ):
        self.channel.exchange_declare(exchange=consumer_exchange_name, exchange_type='direct')
        if secondary_consumer_exchange_name is not None:
            self.channel.exchange_declare(exchange=secondary_consumer_exchange_name, exchange_type='direct')
        for queue_name in consumer_queues_to_recv_from:
            self.channel.queue_declare(queue=queue_name, durable=True)
    
        
    def __configure_producer_bindings(self, 
                               producer_exchange_name: str, 
                               producer_queues_to_bind: dict[str,list[str]]
                               ):
        self.producer_exchange_name = producer_exchange_name
        self.channel.exchange_declare(exchange=producer_exchange_name, exchange_type='direct')
        for queue_name, binding_keys in producer_queues_to_bind.items():
            self.channel.queue_declare(queue=queue_name, durable=True)
            for binding_key in binding_keys:
                self.channel.queue_bind(exchange=producer_exchange_name, queue=queue_name, routing_key=binding_key)


    def set_message_consumer_callback(self, 
                                        queue_name: str, 
                                        main_callback: Callable):
        self.channel.basic_consume(queue=queue_name, on_message_callback=main_callback, auto_ack=True)

    def start_consuming(self):
        self.channel.start_consuming()

    def send_message(self, routing_key: str, msg_body: str):
        self.channel.basic_publish(exchange=self.producer_exchange_name, routing_key=routing_key, body=msg_body, properties=pika.BasicProperties(delivery_mode=pika.DeliveryMode.Persistent), mandatory=True)


    def close_connection(self):
        self.channel.stop_consuming()
        self.connection.close()