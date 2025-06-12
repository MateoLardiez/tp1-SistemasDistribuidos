import logging
from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.middleware_connection_handler import RabbitMQConnectionHandler
import csv
import os

class Query4:

    def __init__(self, id_sinker, number_workers):
        self.query_4_connection = RabbitMQConnectionHandler(
            producer_exchange_name="reports_exchange",
            producer_queues_to_bind={"reports_queue": ["reports_queue"]},
            consumer_exchange_name="joiner_by_credit_id_exchange",
            consumer_queues_to_recv_from=[f"average_credit_aggregated_{id_sinker}"],
        )
        self.query_4_connection.set_message_consumer_callback(f"average_credit_aggregated_{id_sinker}", self.callback)
        self.number_workers = number_workers
        self.clients_state = {}
        self.controller_name = f"sink_query_4_{id_sinker}"

    def start(self):
        logging.info("action: start | result: success | code: Sink_query_4 ")
        self.query_4_connection.start_consuming()

    def callback(self, ch, method, properties, body):
        # id,title,genres,release_date,overview,production_countries,spoken_languages,budget,revenue
        data = MiddlewareMessage.decode_from_bytes(body)

        if data.client_id not in self.clients_state:
            self.clients_state[data.client_id] = {
                "eof_amount": 0, 
                "last_seq_number": 0, 
            }
        if data.controller_name not in self.clients_state[data.client_id]:
            self.clients_state[data.client_id][data.controller_name] = data.seq_number
        elif data.seq_number <= self.clients_state[data.client_id][data.controller_name]:
            logging.warning(f"Duplicated Message {data.client_id} in {data.controller_name} with seq_number {data.seq_number}. Ignoring.")
            return
        
        if data.type != MiddlewareMessageType.EOF_JOINER: 
            lines = data.get_batch_iter_from_payload()
            self.save_data(data.client_id, lines)
        else:
            self.clients_state[data.client_id]["eof_amount"] += 1
            if self.clients_state[data.client_id]["eof_amount"] == self.number_workers:
                self.handler_query_4(data.client_id, data.query_number)
                # Handle EOF message
                msg = MiddlewareMessage(
                    query_number=data.query_number,
                    client_id=data.client_id,
                    seq_number=0,
                    type=MiddlewareMessageType.EOF_RESULT_Q4,
                    payload="EOF",
                    controller_name=self.controller_name
                )
                self.query_4_connection.send_message(
                    routing_key="reports_queue",
                    msg_body=msg.encode_to_str()
                )
                self.clean_temp_files(data.client_id)
                del self.clients_state[data.client_id]

    def handler_query_4(self, client_id, query_number):
        # Ya tengo toda la data en mi csv
        actors = {}
        for line in self.read_data(client_id):
            actor = line[0]
            if actor not in actors:
                actors[actor] = 0
            actors[actor] += int(line[1])  # Cantidad de creditos

        # Ordenar por cantidad de apariciones y luego por orden alfabetico de artistas
        sorted_actors = sorted(actors.items(), key=lambda x: (-x[1], x[0]))
        
        # Limitar a 10 resultados
        result = [[actor, count] for actor, count in sorted_actors[:10]]

        result_csv = MiddlewareMessage.write_csv_batch(result)
        
        msg = MiddlewareMessage(
            query_number=query_number,
            client_id=client_id,
            seq_number=0,
            type=MiddlewareMessageType.RESULT_Q4,
            payload=result_csv,
            controller_name=self.controller_name
        )
        # Send all filtered results in a single message
        self.query_4_connection.send_message(
            routing_key="reports_queue",
            msg_body=msg.encode_to_str()
        )
    
    def clean_temp_files(self, client_id):
        """Elimina los archivos temporales creados para un cliente"""
        files_to_remove = [
            f"query_4-client-{client_id}",
        ]
        
        for file in files_to_remove:
            try:
                if os.path.exists(file):
                    os.remove(file)
                    logging.info(f"action: clean_temp_files | file: {file} | result: removed")
            except Exception as e:
                logging.error(f"action: clean_temp_files | file: {file} | error: {str(e)}")

    def save_data(self, client_id, lines) -> None:
        # logging.info(f"LINEA PARA GUARDAR: {lines}")
        with open(f"query_4-client-{client_id}", 'a+') as file:
            writer = csv.writer(file, quoting=csv.QUOTE_MINIMAL)
            for line in lines:
                writer.writerow(line)

    def read_data(self, client_id):
        with open (f"query_4-client-{client_id}", 'r') as file:
            reader = csv.reader(file, quoting=csv.QUOTE_MINIMAL)
            for row in reader:
                yield row
