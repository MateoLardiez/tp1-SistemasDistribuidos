import logging
from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.middleware_connection_handler import RabbitMQConnectionHandler
import csv

class Query3:

    def __init__(self, number_workers):
        self.query_3_connection = RabbitMQConnectionHandler(
            producer_exchange_name="reports_exchange",
            producer_queues_to_bind={"reports_queue": ["reports_queue"]},
            consumer_exchange_name="joiner_by_rating_id_exchange",
            consumer_queues_to_recv_from=["average_rating_aggregated"]
        )
        self.query_3_connection.set_message_consumer_callback("average_rating_aggregated", self.callback)
        self.number_workers = number_workers
        self.client_state = {}  # {client_id: {"eof_amount": int}}

    def start(self):
        logging.info("action: start | result: success | code: Sink_query_3 ")
        self.query_3_connection.start_consuming()

    def callback(self, ch, method, properties, body):
        # id,title,genres,release_date,overview,production_countries,spoken_languages,budget,revenue
        data = MiddlewareMessage.decode_from_bytes(body)

        if data.type != MiddlewareMessageType.EOF_JOINER:
            lines = data.get_batch_iter_from_payload()
            self.save_data(data.client_id, lines)
        else:
            logging.info("action: EOF | result: success | code: sinker_query_3")
            if not data.client_id in self.client_state:
                # If we don't have the client_id in the state, we need to initialize it
                self.client_state[data.client_id] = {"eof_amount": 0}
            self.client_state[data.client_id]["eof_amount"] += 1
            # Check if we have received all EOF messages
            if self.client_state[data.client_id]["eof_amount"] == self.number_workers:
                self.handler_query_3(data.client_id, data.query_number)
                # Handle EOF message
                msg = MiddlewareMessage(
                    query_number=data.query_number,
                    client_id=data.client_id,
                    type=MiddlewareMessageType.EOF_RESULT_Q3,
                    payload="EOF"
                )
                self.query_3_connection.send_message(
                    routing_key="reports_queue",
                    msg_body=msg.encode_to_str()
                )

    def handler_query_3(self, client_id, query_number):
        # Ya tengo toda la data en mi csv
        joined_results = []
        for line in self.read_data(client_id):
            joined_results.append(line)


        q3_answer = []
        if joined_results:
            joined_results.sort(key=lambda x: x[1])

            worst_rating_movie = joined_results[0]
            best_rating_movie = joined_results[-1]
            logging.info(f"Best movie: {best_rating_movie}, Worst movie: {worst_rating_movie}")
            q3_answer = [best_rating_movie, worst_rating_movie]
        # Join all filtered lines into a single CSV string
        result_csv = MiddlewareMessage.write_csv_batch(q3_answer) # NO ASI
        
        msg = MiddlewareMessage(
            query_number=query_number,
            client_id=client_id,
            type=MiddlewareMessageType.RESULT_Q3,
            payload=result_csv
        )

        # Send all filtered results in a single message
        self.query_3_connection.send_message(
            routing_key="reports_queue",
            msg_body=msg.encode_to_str()
        )
    
    def save_data(self, client_id, lines) -> None:
        # logging.info(f"LINEA PARA GUARDAR: {lines}")
        with open(f"query_3-client-{client_id}", 'a+') as file:
            writer = csv.writer(file, quoting=csv.QUOTE_MINIMAL)
            for line in lines:
                writer.writerow(line)

    def read_data(self, client_id):
        with open (f"query_3-client-{client_id}", 'r') as file:
            reader = csv.reader(file, quoting=csv.QUOTE_MINIMAL)
            for row in reader:
                yield row
