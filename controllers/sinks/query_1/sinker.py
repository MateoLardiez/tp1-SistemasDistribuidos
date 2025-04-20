import pika
import logging
from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType

class Query1:

    def __init__(self):
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        self.channel = self.connection.channel()

    def start(self):
        logging.info("action: start | result: success | code: Sink_query_1 ")
        self.channel.exchange_declare(exchange='results', exchange_type='direct') #Finalmente le envia la respuesta al Gateway
        
        self.channel.exchange_declare(exchange='year_filter', exchange_type='direct')
        result = self.channel.queue_declare(queue='')
        queue_name = result.method.queue
        self.channel.queue_bind(exchange='year_filter', queue=queue_name, routing_key='filter_by_year_result')
        self.channel.basic_consume(queue=queue_name, on_message_callback=self.callback, auto_ack=True)
        self.channel.start_consuming()

    def callback(self, ch, method, properties, body):
        # id,title,genres,release_date,overview,production_countries,spoken_languages,budget,revenue
        data = MiddlewareMessage.decode_from_bytes(body)
        lines = data.get_batch_iter_from_payload()
        self.handler_query_3(lines)

    def handler_query_3(self, lines):
        filtered_lines = []
        for line in lines:
            # logging.info(f"LINEA ---------------> {line}   --------FIN DE LINEA")
            filtered_lines.append([line[1], line[2]])
        
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
            self.channel.basic_publish(exchange='results', routing_key="query_1_completed", body=msg.encode_to_str())
            logging.info(f"action: response_q3 | result: success | count_lines_query: {len(filtered_lines)}")

