import logging
from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.middleware_connection_handler import RabbitMQConnectionHandler
from common.resilient_node import ResilientNode
from common.file_manager import FileManager

SENTIMENT_POS = 0
RATE_POS = 1
AMOUNT_POS = 2

class Query5(ResilientNode):

    def __init__(self, id_sinker, number_workers):
        super().__init__()  # Call parent constructor
        self.rabbitmq_connection_handler = RabbitMQConnectionHandler(
            producer_exchange_name="reports_exchange",
            producer_queues_to_bind={"reports_queue": ["reports_queue"]},
            consumer_exchange_name="group_by_sentiment_exchange",
            consumer_queues_to_recv_from=[f"group_by_sentiment_queue_{id_sinker}"]
        )
        self.rabbitmq_connection_handler.set_message_consumer_callback(f"group_by_sentiment_queue_{id_sinker}", self.callback)
        self.number_workers = number_workers
        self.clients_state = {}
        self.controller_name = f"sinker_query_5_{id_sinker}"
        self.load_state(self.check_files_state)  # Load the state of clients from file

    def start(self):
        logging.info("action: start | result: success | code: Sink_query_5 ")
        try:
            self.rabbitmq_connection_handler.start_consuming()
        except Exception as e:
            logging.info("Consuming stopped")

    def check_files_state(self):
        for client_id in self.clients_state:
            if self.check_file(client_id, "query_5"):
                self.clients_state[client_id]["duplicated_batch"]["query_5"] = True

    def callback(self, ch, method, properties, body):
        # id,title,genres,release_date,overview,production_countries,spoken_languages,budget,revenue
        data = MiddlewareMessage.decode_from_bytes(body)

        if data.client_id not in self.clients_state:
            self.clients_state[data.client_id] = {
                "eof_amount": 0, 
                "last_seq_number": 0, 
                "hash_file": {
                    "query_5": None,
                },
                "duplicated_batch": {
                    "query_5": False,
                },
            }
        if data.controller_name not in self.clients_state[data.client_id]:
            self.clients_state[data.client_id][data.controller_name] = data.seq_number
        elif self.clients_state[data.client_id]["duplicated_batch"]["query_5"]:
            logging.warning(f"Duplicated batch for client {data.client_id} in {data.controller_name}. Ignoring.")
            self.update_duplicate_state(
                data.client_id, "query_5", data.controller_name, data.seq_number
            )
            return
        elif data.seq_number <= self.clients_state[data.client_id][data.controller_name]:
            logging.warning(f"Duplicated Message {data.client_id} in {data.controller_name} with seq_number {data.seq_number}. Ignoring.")
            return
        
        if data.type != MiddlewareMessageType.EOF_MOVIES:
            lines = data.get_batch_iter_from_payload()
            filename = f".data/query_5-client-{data.client_id}"
            self.clients_state[data.client_id]["hash_file"]["query_5"] = self.save_data(filename, lines)
            self.clients_state[data.client_id][data.controller_name] = data.seq_number  # Update the last seq number for this controller
        else:
            self.clients_state[data.client_id]["eof_amount"] += 1
            self.clients_state[data.client_id][data.controller_name] = data.seq_number  # Update the last seq number for this controller
            if self.clients_state[data.client_id]["eof_amount"] == self.number_workers:  # Solo un worker para query 5
                self.handler_query_5(data.client_id, data.query_number)
                msg = MiddlewareMessage(
                    query_number=data.query_number,
                    client_id=data.client_id,
                    seq_number=0,
                    type=MiddlewareMessageType.EOF_RESULT_Q5,
                    payload="EOF",
                    controller_name=self.controller_name
                )
                self.rabbitmq_connection_handler.send_message(
                    routing_key="reports_queue",
                    msg_body=msg.encode_to_str()
                )
                files_to_remove = [
                    f".data/query_5-client-{data.client_id}",
                ]
                FileManager.clean_temp_files(files_to_remove)
                del self.clients_state[data.client_id]
        self.save_state()  # Save the state of clients to file

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
        
        filename = f".data/query_5-client-{client_id}"
        for line in self.read_data(filename):
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
            payload=result_csv,
            controller_name=self.controller_name
        )

        # Send all filtered results in a single message
        self.rabbitmq_connection_handler.send_message(
            routing_key="reports_queue",
            msg_body=msg.encode_to_str()
        )

    def save_data(self, filename, lines) -> None:
        writer = FileManager(filename)
        return writer.save_data(filename, lines)

    def read_data(self, filename):
        reader = FileManager(filename)
        return reader.read()
