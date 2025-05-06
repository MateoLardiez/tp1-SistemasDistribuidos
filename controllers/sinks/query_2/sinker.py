import logging
from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.middleware_connection_handler import RabbitMQConnectionHandler
import csv

QUERY_NUMBER = 2

class Query2:

    def __init__(self):
        self.query_2_connection = RabbitMQConnectionHandler(
            producer_exchange_name="reports_exchange",
            producer_queues_to_bind={"reports_queue": ["reports_queue"]},
            consumer_exchange_name="filter_by_country_invesment_exchange",
            consumer_queues_to_recv_from=["filter_by_country_invesment_queue"],
        )
        self.query_2_connection.set_message_consumer_callback("filter_by_country_invesment_queue", self.callback)
        self.clients_processed = {}

    def start(self):
        logging.info("action: start | result: success | code: Sink_query_2 ")
        self.query_2_connection.start_consuming()

    def callback(self, ch, method, properties, body):
        # id,title,genres,release_date,overview,production_countries,spoken_languages,budget,revenue
        data = MiddlewareMessage.decode_from_bytes(body)
        if data.type != MiddlewareMessageType.EOF_MOVIES:
            if data.client_id not in self.clients_processed:
                self.clients_processed[data.client_id] = {
                    "eof": False, 
                    "seq_number": 0, 
                    "batch_recibidos": 0
                }
            lines = data.get_batch_iter_from_payload()
            self.save_data(data.client_id, lines)
            self.clients_processed[data.client_id]["batch_recibidos"] += 1
            if data.seq_number > self.clients_processed[data.client_id]["seq_number"]:
                self.clients_processed[data.client_id]["seq_number"] = data.seq_number
            if self.clients_processed[data.client_id]["eof"] and self.clients_processed[data.client_id]["seq_number"] - self.clients_processed[data.client_id]["batch_recibidos"] == 0:
                # Si ya se recibió el EOF, no procesamos más mensajes
                self.handler_query_2(data.client_id, data.query_number)

                msg = MiddlewareMessage(
                    query_number=data.query_number,
                    client_id=data.client_id,
                    seq_number=0,
                    type=MiddlewareMessageType.EOF_RESULT_Q2,
                    payload="EOF"
                )
                self.query_2_connection.send_message(
                    routing_key="reports_queue",
                    msg_body=msg.encode_to_str()
                )
        else:
            if data.seq_number-1 - self.clients_processed[data.client_id]["batch_recibidos"] == 0:
                
                self.handler_query_2(data.client_id, data.query_number)
                msg = MiddlewareMessage(
                    query_number=data.query_number,
                    client_id=data.client_id,
                    seq_number=0,
                    type=MiddlewareMessageType.EOF_RESULT_Q2,
                    payload="EOF"
                )
                self.query_2_connection.send_message(
                    routing_key="reports_queue",
                    msg_body=msg.encode_to_str()
                )
            else:
                self.clients_processed[data.client_id]["eof"] = True
                self.clients_processed[data.client_id]["seq_number"] = data.seq_number
                self.clients_processed[data.client_id]["batch_recibidos"] += 1

    def handler_query_2(self, client_id, query_number):
        report_lines = {}
        
        for line in self.read_data(client_id):
            country = line[0].strip("[]").replace("'", "")
            if country not in report_lines:
                report_lines[country] = 0
            report_lines[country] += int(line[1])

        # if report_lines:
            # Sort the dictionary by descending value and get the top 5
        sorted_report_lines = sorted(report_lines.items(), key=lambda x: x[1], reverse=True)[:5]
        sorted_report_lines = [[country, revenue] for country, revenue in sorted_report_lines]
        result_csv = MiddlewareMessage.write_csv_batch(sorted_report_lines)
        
        msg = MiddlewareMessage(
            query_number=query_number,
            client_id=client_id,
            seq_number=0,
            type=MiddlewareMessageType.RESULT_Q2,
            payload=result_csv
        )

        # Send all filtered results in a single message
        self.query_2_connection.send_message(
            routing_key="reports_queue",
            msg_body=msg.encode_to_str()
        )
                
    def save_data(self, client_id, lines) -> None:
        # logging.info(f"LINEA PARA GUARDAR: {lines}")
        with open(f"query_2-client-{client_id}", 'a+') as file:
            writer = csv.writer(file, quoting=csv.QUOTE_MINIMAL)
            for line in lines:
                writer.writerow(line)

    def read_data(self, client_id):
        with open (f"query_2-client-{client_id}", 'r') as file:
            reader = csv.reader(file, quoting=csv.QUOTE_MINIMAL)
            for row in reader:
                yield row
