import logging
from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.defines import QueryNumber
from common.middleware_connection_handler import RabbitMQConnectionHandler
import ast

PROD_COUNTRIES = 5
ID = 0
TITLE = 1
GENRES = 2
YEAR = 3


class FilterByCountry:
    countries: list
    data: object

    def __init__(self, id_worker, number_workers):
        self.id_worker = id_worker
        self.number_workers = number_workers
        self.rabbitmq_connection_handler = RabbitMQConnectionHandler(
            producer_exchange_name="filter_by_country_exchange",
            # producer_queues_to_bind={"country_queue": ["country_queue"]},
            producer_queues_to_bind={
                **{f"country_queue_{i}": [f"country_queue_{i}"] for i in range(self.number_workers)}
            },
            consumer_exchange_name="movies_preprocessor_exchange",
            consumer_queues_to_recv_from=[f"cleaned_movies_queue_country_{self.id_worker}"]
        )        
        # Configurar el callback para la cola específica
        self.rabbitmq_connection_handler.set_message_consumer_callback(f"cleaned_movies_queue_country_{self.id_worker}", self.callback)
        self.countries_query_1 = ["Argentina", "Spain"] 
        self.countries_query_3 = ["Argentina"] 
        self.countries_query_4 = ["Argentina"]
        self.local_state = {}  # Dictionary to store local state of clients
        self.controller_name = f"filter_by_country_{id_worker}"

    def start(self):
        logging.info("action: start | result: success | code: filter_by_country")
        self.rabbitmq_connection_handler.start_consuming()
    
    def callback(self, ch, method, properties, body):
        data = MiddlewareMessage.decode_from_bytes(body)
        if data.client_id not in self.local_state:
            self.local_state[data.client_id] = {
                "last_seq_number": 0,  # This is the last seq number we propagated
                "eof_amount": 0  # This is the number of EOF messages received, when it reaches the number of workers, we can propagate the EOF message
            }
        if data.controller_name not in self.local_state[data.client_id]:
            self.local_state[data.client_id][data.controller_name] = data.seq_number
        elif data.seq_number <= self.local_state[data.client_id][data.controller_name]:
            logging.warning(f"Duplicated Message {data.client_id} in {data.controller_name} with seq_number {data.seq_number}. Ignoring.")
            return
        
        if data.type != MiddlewareMessageType.EOF_MOVIES:
            lines = data.get_batch_iter_from_payload()
            seq_number = self.local_state[data.client_id]["last_seq_number"]
            if data.query_number == QueryNumber.ALL_QUERYS:
                self.handler_all_query(lines, data.client_id, seq_number)
            elif data.query_number == QueryNumber.QUERY_1:
                self.handler_country_filter(lines, self.countries_query_1, data.client_id, data.query_number, seq_number)
            elif data.query_number == QueryNumber.QUERY_3:
                self.handler_country_filter(lines, self.countries_query_3, data.client_id, data.query_number, seq_number)
            elif data.query_number == QueryNumber.QUERY_4:
                self.handler_country_filter(lines, self.countries_query_4, data.client_id, data.query_number, seq_number)
            self.local_state[data.client_id]["last_seq_number"] += 1
        else:
            seq_number = self.local_state[data.client_id]["last_seq_number"]
            self.local_state[data.client_id]["eof_amount"] += 1
            if self.local_state[data.client_id]["eof_amount"] == self.number_workers:
                if data.query_number == QueryNumber.ALL_QUERYS:
                    self.handler_eof_all_querys(data, seq_number)
                else:
                    msg = MiddlewareMessage(
                        query_number=data.query_number,
                        client_id=data.client_id,
                        seq_number=seq_number,
                        type=MiddlewareMessageType.EOF_MOVIES,
                        payload="",
                        controller_name=self.controller_name
                    )
                    self.rabbitmq_connection_handler.send_message(
                        routing_key=f"country_queue_{self.id_worker}",
                        msg_body=msg.encode_to_str()
                    )       
            
    def filter_by_country(self, movie, country_filter):
        countries_of_movie = ast.literal_eval(movie[PROD_COUNTRIES])#<- es un string
        has_countries = all(country in countries_of_movie for country in country_filter)
        return has_countries

    def handler_all_query(self, lines, id_client, seq_number):
        lines_to_filter = []
        for line in lines:
            lines_to_filter.append(line)

        self.handler_country_filter(lines_to_filter, self.countries_query_1, id_client, QueryNumber.QUERY_1, seq_number)
        self.local_state[id_client]["last_seq_number"] += 1
        seq_number = self.local_state[id_client]["last_seq_number"]
        self.handler_country_filter(lines_to_filter, self.countries_query_3, id_client, QueryNumber.QUERY_3, seq_number)
        self.local_state[id_client]["last_seq_number"] += 1
        seq_number = self.local_state[id_client]["last_seq_number"]
        self.handler_country_filter(lines_to_filter, self.countries_query_4, id_client, QueryNumber.QUERY_4, seq_number)
        

    def handler_country_filter(self, lines, countries_filter, id_client, query_number, seq_number):
        filtered_lines = []
        for line in lines:
            if self.filter_by_country(line, countries_filter):
                filtered_lines.append(line)          
        # Join all filtered lines into a single CSV string
        # if filtered_lines:
            # Q1: [title, genres, release_date]
            # Q3: [id, title, release_date]
            # Q4: [id, release_date]
        result_csv = None
        msg = None
        if query_number == QueryNumber.QUERY_1:
            query_result = []
            for line in filtered_lines:
                query_result.append([line[1], line[2], line[3]])
            result_csv = MiddlewareMessage.write_csv_batch(query_result)
            msg = MiddlewareMessage(
                query_number=query_number,
                client_id=id_client,
                seq_number=seq_number,
                type=MiddlewareMessageType.MOVIES_BATCH,
                payload=result_csv,
                controller_name=self.controller_name
            )
        elif query_number == QueryNumber.QUERY_3:
            query_result = []
            for line in filtered_lines:
                query_result.append([line[0], line[1], line[3]])
            result_csv = MiddlewareMessage.write_csv_batch(query_result)
            msg = MiddlewareMessage(
                query_number=query_number,
                client_id=id_client,
                seq_number=seq_number,
                type=MiddlewareMessageType.MOVIES_BATCH,
                payload=result_csv,
                controller_name=self.controller_name
            )
        elif query_number == QueryNumber.QUERY_4:
            query_result = []
            for line in filtered_lines:
                query_result.append([line[0], line[3]])
            result_csv = MiddlewareMessage.write_csv_batch(query_result)
            msg = MiddlewareMessage(
                query_number=query_number,
                client_id=id_client,
                seq_number=seq_number,
                type=MiddlewareMessageType.MOVIES_BATCH,
                payload=result_csv,
                controller_name=self.controller_name
            )
        
        id_worker = seq_number % self.number_workers
        self.rabbitmq_connection_handler.send_message(
            routing_key=f"country_queue_{id_worker}",
            msg_body=msg.encode_to_str()
        )

    def handler_eof_all_querys(self, data, seq_number):
        initial_seq_number = seq_number
        for query_number in [QueryNumber.QUERY_1, QueryNumber.QUERY_3, QueryNumber.QUERY_4]:
            for id_worker in range(self.number_workers):
                msg = MiddlewareMessage(
                    query_number=query_number,
                    client_id=data.client_id,
                    seq_number=initial_seq_number,
                    type=MiddlewareMessageType.EOF_MOVIES,
                    payload="",
                    controller_name=self.controller_name
                )
                # Send EOF message to all workers
                self.rabbitmq_connection_handler.send_message(
                    routing_key=f"country_queue_{id_worker}",
                    msg_body=msg.encode_to_str()
                )
                initial_seq_number += 1
