import logging
from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.middleware_connection_handler import RabbitMQConnectionHandler
import csv
import os

SENTIMENT_POS = 0
RATE_POS = 1
AMOUNT_POS = 2

class Query5:

    def __init__(self, id_sinker, number_workers):
        self.query_5_connection = RabbitMQConnectionHandler(
            producer_exchange_name="reports_exchange",
            producer_queues_to_bind={"reports_queue": ["reports_queue"]},
            consumer_exchange_name="group_by_sentiment_exchange",
            consumer_queues_to_recv_from=[f"group_by_sentiment_queue_{id_sinker}"]
        )
        self.query_5_connection.set_message_consumer_callback(f"group_by_sentiment_queue_{id_sinker}", self.callback)
        self.number_workers = number_workers
        self.clients_processed = {}

    def start(self):
        logging.info("action: start | result: success | code: Sink_query_5 ")
        self.query_5_connection.start_consuming()

    def callback(self, ch, method, properties, body):
        # id,title,genres,release_date,overview,production_countries,spoken_languages,budget,revenue
        data = MiddlewareMessage.decode_from_bytes(body)

        if data.type != MiddlewareMessageType.EOF_MOVIES:
            # if data.client_id not in self.clients_processed:
            #     self.clients_processed[data.client_id] = {
            #         "eof": False, 
            #         "seq_number": 0, 
            #         "batch_recibidos": 0
            #     }
            lines = data.get_batch_iter_from_payload()
            self.save_data(data.client_id, lines)
            # self.clients_processed[data.client_id]["batch_recibidos"] += 1
            # if data.seq_number > self.clients_processed[data.client_id]["seq_number"]:
            #     self.clients_processed[data.client_id]["seq_number"] = data.seq_number
            # if self.clients_processed[data.client_id]["eof"] and self.clients_processed[data.client_id]["seq_number"] - self.clients_processed[data.client_id]["batch_recibidos"] == 0:
            #     # Si ya se recibió el EOF, no procesamos más mensajes
            #     self.handler_query_5(data.client_id, data.query_number)

            #     msg = MiddlewareMessage(
            #         query_number=data.query_number,
            #         client_id=data.client_id,
            #         seq_number=0,
            #         type=MiddlewareMessageType.EOF_RESULT_Q5,
            #         payload="EOF"
            #     )
            #     self.query_5_connection.send_message(
            #         routing_key="reports_queue",
            #         msg_body=msg.encode_to_str()
            #     )
            #     self.clean_temp_files(data.client_id)
            #     del self.clients_processed[data.client_id]
            # nos quedamos con el seq number mas grande
        else:
            # if data.seq_number-1 - self.clients_processed[data.client_id]["batch_recibidos"] == 0:
            if not data.client_id in self.clients_processed:
                self.clients_processed[data.client_id] = {"eof_amount": 0}
            self.clients_processed[data.client_id]["eof_amount"] += 1

            if self.clients_processed[data.client_id]["eof_amount"] == self.number_workers:  # Solo un worker para query 5
                self.handler_query_5(data.client_id, data.query_number)
                msg = MiddlewareMessage(
                    query_number=data.query_number,
                    client_id=data.client_id,
                    seq_number=0,
                    type=MiddlewareMessageType.EOF_RESULT_Q5,
                    payload="EOF"
                )
                self.query_5_connection.send_message(
                    routing_key="reports_queue",
                    msg_body=msg.encode_to_str()
                )
                self.clean_temp_files(data.client_id)
                del self.clients_processed[data.client_id]
            # else:
            #     self.clients_processed[data.client_id]["eof"] = True
            #     self.clients_processed[data.client_id]["seq_number"] = data.seq_number
            #     self.clients_processed[data.client_id]["batch_recibidos"] += 1


    def handler_query_5(self, client_id, query_number):
        # Ya tengo toda la data en mi csv
        sentiment_groups = {
            "POSITIVE": {
                "acumulator": 0,
                "count": 0
            },
            "NEGATIVE": {
                "acumulator": 0,
                "count": 0
            }
        }
                
        for line in self.read_data(client_id):
            sentiment = line[SENTIMENT_POS]
            rate = float(line[RATE_POS])
            amount = int(line[AMOUNT_POS])
            sentiment_groups[sentiment]["acumulator"] += rate
            sentiment_groups[sentiment]["count"] += amount

        average_rate_by_sentiment = {
            sentiment: (sentiment_groups[sentiment]["acumulator"] / sentiment_groups[sentiment]["count"]) if sentiment_groups[sentiment]["count"] > 0 else 0
            for sentiment in sentiment_groups
        }

        q5_answer = []
        # Verificación correcta si hay datos en las listas del diccionario
        if sentiment_groups["POSITIVE"]["count"] > 0 or sentiment_groups["NEGATIVE"]["count"] > 0:
            q5_answer = [
                ["POSITIVE", average_rate_by_sentiment["POSITIVE"]],
                ["NEGATIVE", average_rate_by_sentiment["NEGATIVE"]]
            ]

        # logging.info(f"QUERY 5: POSITIVE: {q5_answer[1][1]}, NEGATIVE: {q5_answer[2][1]}")
        # Join all filtered lines into a single CSV string
        result_csv = MiddlewareMessage.write_csv_batch(q5_answer) # NO ASI
        
        msg = MiddlewareMessage(
            query_number=query_number,
            client_id=client_id,
            seq_number=0,
            type=MiddlewareMessageType.RESULT_Q5,
            payload=result_csv
        )

        # Send all filtered results in a single message
        self.query_5_connection.send_message(
            routing_key="reports_queue",
            msg_body=msg.encode_to_str()
        )
    
    def clean_temp_files(self, client_id):
        """Elimina los archivos temporales creados para un cliente"""
        files_to_remove = [
            f"query_5-client-{client_id}",
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
        with open(f"query_5-client-{client_id}", 'a+') as file:
            writer = csv.writer(file, quoting=csv.QUOTE_MINIMAL)
            for line in lines:
                writer.writerow(line)

    def read_data(self, client_id):
        with open (f"query_5-client-{client_id}", 'r') as file:
            reader = csv.reader(file, quoting=csv.QUOTE_MINIMAL)
            for row in reader:
                yield row
