import logging
from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.middleware_connection_handler import RabbitMQConnectionHandler

TITLE = 0
GENRES = 1

class Query1:

    def __init__(self):
        self.query_1_connection = RabbitMQConnectionHandler(
            producer_exchange_name="reports_exchange",
            producer_queues_to_bind={"reports_queue": ["reports_queue"]},
            consumer_exchange_name="filter_by_year_exchange",
            consumer_queues_to_recv_from=["sink_query_1_queue"]
        )
        self.query_1_connection.set_message_consumer_callback("sink_query_1_queue", self.callback)

    def start(self):
        logging.info("action: start | result: success | code: Sink_query_1 ")
        self.query_1_connection.start_consuming()

    def callback(self, ch, method, properties, body):
        # id,title,genres,release_date,overview,production_countries,spoken_languages,budget,revenue
        data = MiddlewareMessage.decode_from_bytes(body)
        if data.type != MiddlewareMessageType.EOF_MOVIES:
            lines = data.get_batch_iter_from_payload()
            self.handler_query_1(lines, data.client_id, data.query_number)
        else:
            # Handle EOF message
            msg = MiddlewareMessage(
                query_number=data.query_number,
                client_id=data.client_id,
                type=MiddlewareMessageType.EOF_RESULT_Q1,
                payload="EOF"
            )
            self.query_1_connection.send_message(
                routing_key="reports_queue",
                msg_body=msg.encode_to_str()
            )


    def handler_query_1(self, lines, client_id, query_number):
        filtered_lines = []
        for line in lines:
            filtered_lines.append([line[TITLE], line[GENRES]])
        
        # if filtered_lines:
            # Join all filtered lines into a single CSV string
        result_csv = MiddlewareMessage.write_csv_batch(filtered_lines)
        
        msg = MiddlewareMessage(
            query_number=query_number,
            client_id=client_id,
            type=MiddlewareMessageType.RESULT_Q1,
            payload=result_csv
        )

        # Send all filtered results in a single message
        self.query_1_connection.send_message(
            routing_key="reports_queue",
            msg_body=msg.encode_to_str()
        )
     
