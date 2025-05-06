import logging

from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.defines import QueryNumber
from common.middleware_connection_handler import RabbitMQConnectionHandler

BUDGET = 1
REVENUE = 2

# Columns needed: ["id", "title", "overview", "budget", "revenue"]

class AggregatorRB:
    data: object
    def __init__(self):
        self.data = ""
        self.aggregator_r_b_connection = RabbitMQConnectionHandler(
            producer_exchange_name="aggregator_r_b_exchange",
            producer_queues_to_bind={ "aggregated_r_b_data_queue": ["aggregated_r_b_data_queue"]},
            consumer_exchange_name="aggregator_nlp_exchange",
            consumer_queues_to_recv_from=["aggregated_nlp_data_queue"]
        )
        self.aggregator_r_b_connection.set_message_consumer_callback("aggregated_nlp_data_queue", self.callback)

    def start(self):
        logging.info("action: start | result: success | code: aggregator_r_b")
        self.aggregator_r_b_connection.start_consuming()

    def callback(self, ch, method, properties, body):

        data = MiddlewareMessage.decode_from_bytes(body)
        lines = data.get_batch_iter_from_payload()
        if data.type != MiddlewareMessageType.EOF_MOVIES:
            self.handler_aggregator_query_5(lines, data.client_id, data.seq_number, data.query_number)
        else:
            msg = MiddlewareMessage(
                query_number=data.query_number,
                client_id=data.client_id,
                seq_number=data.seq_number,
                type=MiddlewareMessageType.EOF_MOVIES,
                payload=""
            )
            self.aggregator_r_b_connection.send_message(
                routing_key="aggregated_r_b_data_queue",
                msg_body=msg.encode_to_str()
            )
        
    def aggregator_r_b(self, movie):
        try:
            revenue = float(movie[REVENUE])
            budget = float(movie[BUDGET])
            rate = revenue / budget
            
            return True, rate
        except (IndexError, ValueError):
            logging.error(f"Invalid release date format for movie: {movie}")
            return False, 0

    def handler_aggregator_query_5(self, lines, client_id, seq_number, query_number):
        filtered_lines = []
        for line in lines:
            could_aggregate, rate_value = self.aggregator_r_b(line)
            if could_aggregate:
                filtered_line = []
                # Agregar el rate
                filtered_line.append(line[0])
                filtered_line.append(str(rate_value))
                filtered_lines.append(filtered_line)

        # if filtered_lines:
            # Join all filtered lines into a single CSV string
        result_csv = MiddlewareMessage.write_csv_batch(filtered_lines)
        msg = MiddlewareMessage(
            query_number=query_number,
            client_id=client_id,
            seq_number=seq_number,
            type=MiddlewareMessageType.MOVIES_BATCH,
            payload=result_csv
        )
        self.aggregator_r_b_connection.send_message(
            routing_key="aggregated_r_b_data_queue",
            msg_body=msg.encode_to_str()
        )
        
