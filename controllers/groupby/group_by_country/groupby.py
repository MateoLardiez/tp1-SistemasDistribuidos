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

    def __init__(self, numberSinkers):
        self.group_by_country_connection = RabbitMQConnectionHandler(
            producer_exchange_name="group_by_country_exchange",
            producer_queues_to_bind={
                **{f"group_by_country_queue_{i}": [f"group_by_country_queue_{i}"] for i in range(numberSinkers)}
            },
            consumer_exchange_name="filter_by_country_invesment_exchange",
            consumer_queues_to_recv_from=["filter_by_country_invesment_queue"],
        )        
        # Configurar el callback para la cola específica
        self.group_by_country_connection.set_message_consumer_callback("filter_by_country_invesment_queue", self.callback)
        self.numberSinkers = numberSinkers

    def start(self):
        logging.info("action: start | result: success | code: filter_by_country")
        self.group_by_country_connection.start_consuming()
    
    def callback(self, ch, method, properties, body):
        data = MiddlewareMessage.decode_from_bytes(body)
        if data.type != MiddlewareMessageType.EOF_MOVIES:
            lines = data.get_batch_iter_from_payload()
            self.handler_country_group_by(lines, data.client_id, data.query_number, data.seq_number)
        else:
            sinker_number = data.client_id % self.numberSinkers
            msg = MiddlewareMessage(
                query_number=data.query_number,
                client_id=data.client_id,
                seq_number=data.seq_number,
                type=MiddlewareMessageType.EOF_MOVIES,
                payload=""
            )
            self.group_by_country_connection.send_message(
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
            payload=result_csv
        )
       
        sinker_number = id_client % self.numberSinkers
        self.group_by_country_connection.send_message(
            routing_key=f"group_by_country_queue_{sinker_number}",
            msg_body=msg.encode_to_str()
        )
