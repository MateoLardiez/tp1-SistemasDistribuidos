import logging
from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.middleware_connection_handler import RabbitMQConnectionHandler
import csv

SENTIMENT_POS = 0
RATE_POS = 1

class Query5:

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

        if data.type != MiddlewareMessageType.EOF_MOVIES:
            lines = data.get_batch_iter_from_payload()
            self.save_data(data.client_id, lines)
        else:
            logging.info("action: EOF | result: success | code: sinker_query_5")
            self.handler_query_5(data.client_id, data.query_number)
            # Handle EOF message
            msg = MiddlewareMessage(
                query_number=data.query_number,
                client_id=data.client_id,
                type=MiddlewareMessageType.EOF_RESULT_Q5,
                payload="EOF"
            )
            self.query_5_connection.send_message(
                routing_key="reports_queue",
                msg_body=msg.encode_to_str()
            )

    def handler_query_5(self, client_id, query_number):
        # Ya tengo toda la data en mi csv
        sentiment_groups = {"POSITIVE": [], "NEGATIVE": []}
                
        for line in self.read_data(client_id):
            sentiment = line[SENTIMENT_POS]
            rate = float(line[RATE_POS])
            sentiment_groups[sentiment].append(rate)


        average_rate_by_sentiment = {
            sentiment: (sum(rates) / len(rates)) if rates else 0
            for sentiment, rates in sentiment_groups.items()
        }

        q5_answer = []
        if not average_rate_by_sentiment.is_empty():
            q5_answer = [
                ["sentiment", "average_rate"],
                ["POSITIVE", average_rate_by_sentiment["POSITIVE"]],
                ["NEGATIVE", average_rate_by_sentiment["NEGATIVE"]]
            ]

        # logging.info(f"QUERY 5: POSITIVE: {q5_answer[1][1]}, NEGATIVE: {q5_answer[2][1]}")
        # Join all filtered lines into a single CSV string
        result_csv = MiddlewareMessage.write_csv_batch(q5_answer) # NO ASI
        
        msg = MiddlewareMessage(
            query_number=query_number,
            client_id=client_id,
            type=MiddlewareMessageType.RESULT_Q5,
            payload=result_csv
        )

        # Send all filtered results in a single message
        self.query_5_connection.send_message(
            routing_key="reports_queue",
            msg_body=msg.encode_to_str()
        )
    
    def save_data(self, client_id, lines) -> None:
        # logging.info(f"LINEA PARA GUARDAR: {lines}")
        with open(f"query_5-client-{client_id}", 'a+') as file:
            writer = csv.writer(file, quoting=csv.QUOTE_MINIMAL)
            for line in lines:
                writer.writerow(line)

    def read_data(self, client_id):
        with open (f"query_5-client-{client_id}", 'r') as file:
            reader = csv.reader(file, quoting=csv.QUOTE_MINIMAL)
            for row in reader:
                yield row
