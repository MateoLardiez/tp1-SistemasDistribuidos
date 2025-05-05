import logging
from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.middleware_connection_handler import RabbitMQConnectionHandler
import csv

class Query4:

    def __init__(self, number_workers):
        self.query_4_connection = RabbitMQConnectionHandler(
            producer_exchange_name="reports_exchange",
            producer_queues_to_bind={"reports_queue": ["reports_queue"]},
            consumer_exchange_name="joiner_by_credit_id_exchange",
            consumer_queues_to_recv_from=["average_credit_aggregated"]
        )
        self.query_4_connection.set_message_consumer_callback("average_credit_aggregated", self.callback)
        self.number_workers = number_workers
        self.client_state = {}

    def start(self):
        logging.info("action: start | result: success | code: Sink_query_4 ")
        self.query_4_connection.start_consuming()

    def callback(self, ch, method, properties, body):
        # id,title,genres,release_date,overview,production_countries,spoken_languages,budget,revenue
        data = MiddlewareMessage.decode_from_bytes(body)

        if data.type != MiddlewareMessageType.EOF_JOINER:
            lines = data.get_batch_iter_from_payload()
            self.save_data(data.client_id, lines)
        else:
            logging.info("action: EOF | result: success | code: sinker_query_4")

            if not data.client_id in self.client_state:
                # If we don't have the client_id in the state, we need to initialize it
                self.client_state[data.client_id] = {"eof_amount": 0}
            self.client_state[data.client_id]["eof_amount"] += 1
            # Check if we have received all EOF messages
            if self.client_state[data.client_id]["eof_amount"] == self.number_workers:
                self.handler_query_4(data.client_id, data.query_number)
                # Handle EOF message
                msg = MiddlewareMessage(
                    query_number=data.query_number,
                    client_id=data.client_id,
                    type=MiddlewareMessageType.EOF_RESULT_Q4,
                    payload="EOF"
                )
                self.query_4_connection.send_message(
                    routing_key="reports_queue",
                    msg_body=msg.encode_to_str()
                )

    def handler_query_4(self, client_id, query_number):
        # Ya tengo toda la data en mi csv
        results = []
        for line in self.read_data(client_id):
            actor = line[0]
            movies_list = line[1].strip("[]").replace("'", "").split(", ")
            count_movies = len(movies_list)
            q4_answer = [actor, count_movies]
            results.append(q4_answer)

        # Ordenar por cantidad de apariciones y luego por orden alfabetico de artistas
        results = sorted(results, key=lambda x: (-x[1], x[0]))
        
        # Limitar a 10 resultados
        results = results[:10]

        result_csv = MiddlewareMessage.write_csv_batch(results) # NO ASI
        
        msg = MiddlewareMessage(
            query_number=query_number,
            client_id=client_id,
            type=MiddlewareMessageType.RESULT_Q4,
            payload=result_csv
        )

        # Send all filtered results in a single message
        self.query_4_connection.send_message(
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
