import logging
from common.resilient_node import ResilientNode
from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.defines import QueryNumber
from common.middleware_connection_handler import RabbitMQConnectionHandler

YEAR_Q1 = 2 # release_date position
YEAR_Q3 = 2 # release_date position for query 3
YEAR_Q4 = 1 # release_date position for query 4
class FilterByYear(ResilientNode):
    year: int
    data: object

    def __init__(self, number_workers, number_sinkers, id_worker):
        super().__init__()  # Call parent constructor
        self.id_worker = id_worker
        self.year_range_query_1 = (2000, 2009)
        self.year_range_query_3 = (2000, None)
        self.year_range_query_4 = (2000, None)
        
        # self.data = ""
        self.rabbitmq_connection_handler = RabbitMQConnectionHandler(
            producer_exchange_name="filter_by_year_exchange",
            producer_queues_to_bind={
                **{f"sink_query_1_queue_{i}": [f"sink_query_1_queue_{i}"] for i in range(number_sinkers)},
                **{f"joiner_by_ratings_movies_queue_{i}": [f"joiner_by_ratings_movies_queue_{i}"] for i in range(number_workers)},
                **{f"joiner_by_credits_movies_queue_{i}": [f"joiner_by_credits_movies_queue_{i}"] for i in range(number_workers)},
            },
            consumer_exchange_name="filter_by_country_exchange",
            consumer_queues_to_recv_from=[f"country_queue_{id_worker}"],
        )
        self.rabbitmq_connection_handler.set_message_consumer_callback(f"country_queue_{id_worker}", self.callback)
        self.number_workers = number_workers
        self.number_sinkers = number_sinkers
        self.local_state = {}  # Dictionary to store local state of clients
        self.controller_name = f"filter_by_year_{id_worker}"

    def start(self):
        logging.info("action: start | result: success | code: filter_by_year")
        self.rabbitmq_connection_handler.start_consuming()

    def callback(self, ch, method, properties, body):
        data = MiddlewareMessage.decode_from_bytes(body)
        if data.client_id not in self.local_state:
            self.local_state[data.client_id] = {
                "last_seq_number": 0,  # This is the last seq number we propagated
                "eof_amount":{
                    QueryNumber.QUERY_1: 0, 
                    QueryNumber.QUERY_3: 0,
                    QueryNumber.QUERY_4: 0
                },  # This is the number of EOF messages received for each query, when it reaches the number of workers, we can propagate the EOF message
                # This is the number of EOF messages received, when it reaches the number of workers, we can propagate the EOF message   
            }
        if data.controller_name not in self.local_state[data.client_id]:
            self.local_state[data.client_id][data.controller_name] = data.seq_number
        elif data.seq_number <= self.local_state[data.client_id][data.controller_name]:
            logging.warning(f"Duplicated Message {data.client_id} in {data.controller_name} with seq_number {data.seq_number}. Ignoring.")
            return
        
        if data.type != MiddlewareMessageType.EOF_MOVIES:    
            lines = data.get_batch_iter_from_payload()
            seq_number = self.local_state[data.client_id]["last_seq_number"]
            if data.query_number == QueryNumber.QUERY_1:
                self.handler_year_filter(lines, self.year_range_query_1, data.query_number, data.client_id, YEAR_Q1, seq_number)
            elif data.query_number == QueryNumber.QUERY_3:
                self.handler_year_filter(lines, self.year_range_query_3, data.query_number, data.client_id, YEAR_Q3, seq_number)
            elif data.query_number == QueryNumber.QUERY_4:
                self.handler_year_filter(lines, self.year_range_query_4, data.query_number, data.client_id, YEAR_Q4, seq_number)
            self.local_state[data.client_id]["last_seq_number"] += 1
        else:
            seq_number = self.local_state[data.client_id]["last_seq_number"]
            self.local_state[data.client_id]["eof_amount"][data.query_number] += 1
            if self.local_state[data.client_id]["eof_amount"][data.query_number] == self.number_workers:
                msg = MiddlewareMessage(
                    query_number=data.query_number,
                    client_id=data.client_id,
                    seq_number=seq_number,
                    type=MiddlewareMessageType.EOF_MOVIES,
                    payload="",
                    controller_name=self.controller_name
                )
                if data.query_number == QueryNumber.QUERY_1:
                    sinker_id = data.client_id % self.number_sinkers
                    self.rabbitmq_connection_handler.send_message(
                            routing_key=f"sink_query_1_queue_{sinker_id}",
                            msg_body=msg.encode_to_str()
                    )
                    
                elif data.query_number == QueryNumber.QUERY_3:
                    for i in range(self.number_workers):
                        msg = MiddlewareMessage(
                            query_number=data.query_number,
                            client_id=data.client_id,
                            seq_number=seq_number,
                            type=MiddlewareMessageType.EOF_MOVIES,
                            payload="",
                            controller_name=self.controller_name
                        )
                        self.rabbitmq_connection_handler.send_message(
                            routing_key=f"joiner_by_ratings_movies_queue_{i}",
                            msg_body=msg.encode_to_str()
                        )
                elif data.query_number == QueryNumber.QUERY_4:
                    for i in range(self.number_workers):
                        msg = MiddlewareMessage(
                            query_number=data.query_number,
                            client_id=data.client_id,
                            seq_number=seq_number,
                            type=MiddlewareMessageType.EOF_MOVIES,
                            payload="",
                            controller_name=self.controller_name
                        )
                        self.rabbitmq_connection_handler.send_message(
                            routing_key=f"joiner_by_credits_movies_queue_{i}",
                            msg_body=msg.encode_to_str()
                        )
    
    def filter_by_year(self, movie, year_filter, year_pos):
        if not movie[year_pos]:
            return False
        
        year_of_movie = movie[year_pos]
        try:
            release_year = int(year_of_movie.split('-')[0])
            if isinstance(year_filter, tuple):
                start, end = year_filter

                if start is not None and end is not None:
                    return start <= release_year <= end
                elif start is not None:
                    return release_year >= start
                elif end is not None:
                    return release_year <= end
                else:
                    return True  # (None, None) → no filtro
            else:
                return release_year == year_filter
        except (IndexError, ValueError):
            logging.error(f"Invalid release date format for movie: {movie}")
            return False

    def handler_year_filter(self, lines, year_filter, query_number, client_id, year_pos, seq_number):
        filtered_lines = []
        for line in lines:
            if self.filter_by_year(line, year_filter, year_pos):
                filtered_lines.append(line)
                
        query_result = []
        result_csv = None
        # Entradas
        # Q1: [title, genres, release_date]
        # Q3: [id, title, release_date]
        # Q4: [id, release_date]
        # Salidas:
        # Q1: [title, genres]
        # Q3: [id, title]
        # Q4: [id]
        if query_number == QueryNumber.QUERY_1:
            for line in filtered_lines:
                query_result.append([line[0], line[1]])
            sinker_id = client_id % self.number_sinkers
            result_csv = MiddlewareMessage.write_csv_batch(query_result) 
            self.send_message_queue(
                routing_key=f"sink_query_1_queue_{sinker_id}",
                data=result_csv,
                seq_number=seq_number,
                query_number=query_number,
                client_id=client_id
            )
        elif query_number == QueryNumber.QUERY_3:
            sharding_data = {}
            for line in filtered_lines:
                sharding_key = int(line[0]) % self.number_workers
                if sharding_key not in sharding_data:
                    sharding_data[sharding_key] = []
                sharding_data[sharding_key].append([line[0], line[1]])

            for key, value in sharding_data.items():
                result_csv = MiddlewareMessage.write_csv_batch([value])
                self.send_message_queue(
                    routing_key=f"joiner_by_ratings_movies_queue_{key}",
                    data=result_csv,
                    seq_number=seq_number,
                    query_number=query_number,
                    client_id=client_id
                )
        elif query_number == QueryNumber.QUERY_4:
            sharding_data = {}
            for line in filtered_lines:
                sharding_key = int(line[0]) % self.number_workers
                if sharding_key not in sharding_data:
                    sharding_data[sharding_key] = []
                sharding_data[sharding_key].append(line[0])

            for key, value in sharding_data.items():
                result_csv = MiddlewareMessage.write_csv_batch([value])
                self.send_message_queue(
                    routing_key=f"joiner_by_credits_movies_queue_{key}",
                    data=result_csv,
                    seq_number=seq_number,
                    query_number=query_number,
                    client_id=client_id
                )


    def send_message_queue(self, routing_key, data, query_number, client_id, seq_number):
        msg = MiddlewareMessage(
            query_number=query_number,
            client_id=client_id,
            seq_number=seq_number,
            type=MiddlewareMessageType.MOVIES_BATCH,
            payload=data,
            controller_name=self.controller_name
        )
        self.rabbitmq_connection_handler.send_message(
            routing_key=routing_key,
            msg_body=msg.encode_to_str()
        )