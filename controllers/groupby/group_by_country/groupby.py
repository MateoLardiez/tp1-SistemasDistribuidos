import logging
from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.defines import QueryNumber
from common.middleware_connection_handler import RabbitMQConnectionHandler

PROD_COUNTRIES = 5
ID = 0
TITLE = 1
GENRES = 2
YEAR = 3


class GroupByCountry:
    countries: list
    data: object

    def __init__(self, number_sinkers, id_worker, number_workers):
        self.id_worker = id_worker
        self.number_workers = number_workers
        self.rabbitmq_connection_handler = RabbitMQConnectionHandler(
            producer_exchange_name="group_by_country_exchange",
            producer_queues_to_bind={
                **{f"group_by_country_queue_{i}": [f"group_by_country_queue_{i}"] for i in range(number_sinkers)}
            },
            consumer_exchange_name="filter_by_country_invesment_exchange",
            consumer_queues_to_recv_from=[f"filter_by_country_invesment_queue_{self.id_worker}"],
        )
        # Configurar el callback para la cola específica
        self.rabbitmq_connection_handler.set_message_consumer_callback(f"filter_by_country_invesment_queue_{self.id_worker}", self.callback)
        self.number_sinkers = number_sinkers
        self.local_state = {}  # Diccionario para almacenar el estado local de los clientes
        self.controller_name = f"group_by_country_{id_worker}"

    def start(self):
        logging.info("action: start | result: success | code: filter_by_country")
        self.rabbitmq_connection_handler.start_consuming()
    
    def callback(self, ch, method, properties, body):
        data = MiddlewareMessage.decode_from_bytes(body)
        if data.client_id not in self.local_state:
                self.local_state[data.client_id] = {
                    "last_seq_number": 0, # Este es el último seq number que propagamos
                    "eof_amount": 0 # This is the number of EOF messages received, when it reaches the number of workers, we can propagate the EOF message
                }
        if data.controller_name not in self.local_state[data.client_id]:
            self.local_state[data.client_id][data.controller_name] = data.seq_number # Este es el seq number que recibimos
        elif data.seq_number <= self.local_state[data.client_id][data.controller_name]:
            logging.warning(f"Duplicated Message {data.client_id} in {data.controller_name} with seq_number {data.seq_number}. Ignoring.")
            return
        
        if data.type != MiddlewareMessageType.EOF_MOVIES:
                
            lines = data.get_batch_iter_from_payload()
            seq_number = self.local_state[data.client_id]["last_seq_number"]
            self.handler_country_group_by(lines, data.client_id, data.query_number, seq_number)

            self.local_state[data.client_id]["last_seq_number"] += 1

        else:
            seq_number = self.local_state[data.client_id]["last_seq_number"]
            self.local_state[data.client_id]["eof_amount"] += 1
            
            if self.local_state[data.client_id]["eof_amount"] == self.number_workers:
                sinker_number = data.client_id % self.number_sinkers
                msg = MiddlewareMessage(
                    query_number=data.query_number,
                    client_id=data.client_id,
                    seq_number=seq_number,
                    type=MiddlewareMessageType.EOF_MOVIES,
                    payload="",
                    controller_name=self.controller_name
                )
                self.rabbitmq_connection_handler.send_message(
                    routing_key=f"group_by_country_queue_{sinker_number}",
                    msg_body=msg.encode_to_str()
                )

    def handler_country_group_by(self, lines, id_client, query_number, seq_number):
        agrouped_lines = []
        country_group_by = {}
        for line in lines:
            country = line[0].strip("[]").replace("'", "")
            if country not in country_group_by:
                country_group_by[country] = 0
            country_group_by[country] += int(line[1])
        
        [agrouped_lines.append([country, country_group_by[country]]) for country in country_group_by]
        result_csv = MiddlewareMessage.write_csv_batch(agrouped_lines)
        msg = MiddlewareMessage(
            query_number=query_number,
            client_id=id_client,
            seq_number=seq_number,
            type=MiddlewareMessageType.MOVIES_BATCH,
            payload=result_csv,
            controller_name=self.controller_name
        )
       
        sinker_number = id_client % self.number_sinkers
        self.rabbitmq_connection_handler.send_message(
            routing_key=f"group_by_country_queue_{sinker_number}",
            msg_body=msg.encode_to_str()
        )
