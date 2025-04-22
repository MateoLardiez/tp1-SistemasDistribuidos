import logging
from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.middleware_connection_handler import RabbitMQConnectionHandler

class Query1:

    def __init__(self):
        self.query_5_connection = RabbitMQConnectionHandler(
            producer_exchange_name="reports_exchange",
            producer_queues_to_bind={"reports_queue": ["reports_queue"]},
            consumer_exchange_name="aggregator_r_b_exchange",
            consumer_queues_to_recv_from=["aggregated_r_b_data_queue"]
        )
        self.query_5_connection.set_message_consumer_callback("aggregated_r_b_data_queue", self.callback)

    def start(self):
        logging.info("action: start | result: success | code: Sink_query_5 ")
        self.query_5_connection.start_consuming()

    def callback(self, ch, method, properties, body):
        # id,title,genres,release_date,overview,production_countries,spoken_languages,budget,revenue
        data = MiddlewareMessage.decode_from_bytes(body)
        lines = data.get_batch_iter_from_payload()
        self.handler_query_3(lines)

    def handler_query_3(self, lines):
        filtered_lines = []
        for line in lines:
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
            self.query_5_connection.send_message(
                routing_key="reports_queue_5",
                msg_body=msg.encode_to_str()
            )
     
