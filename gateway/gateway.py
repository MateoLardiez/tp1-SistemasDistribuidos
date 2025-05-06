import socket
import logging
import signal
import multiprocessing
from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType
from common.message_protocol import MessageProtocol
from common.defines import ClientCommunication
from common.middleware_connection_handler import RabbitMQConnectionHandler

CODE_ALL_QUERYS = 0
CODE_BATCH = 6
CODE_RESULT = b'R'
CODE_END = 10
CODE_WAIT = b'W'
CODE_WINNER = b'S'
SIZE_BATCH = 8 * 1024

ALL_QUERY=0
QUERY_1 = 1
# File batch types
BATCH_MOVIES = 6
BATCH_RATINGS = 7
BATCH_CREDITS = 8
BATCH_END = 9

class Gateway:
    def __init__(self, port, listen_backlog, clients):
        # Initialize server socket
        self._server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._server_socket.bind(('', port))
        self._server_socket.listen(listen_backlog)
        self.serverIsAlive = True
        self.manager = multiprocessing.Manager()
        self.clients = self.manager.dict()
        self.clients_lock = multiprocessing.Lock()
        
        self.consumer_exchange_name = "reports_exchange"
        self.consumer_queue = "reports_queue"
        self.producer_exchange_name = "gateway_exchange"
        self.producer_queue_of_movies = "movies_queue"
        self.producer_queue_of_ratings = "ratings_queue"
        self.producer_queue_of_credits = "credits_queue"
        self.publisher_connection = None
        self.clients_batch_received = {}

    def set_signals(self):
        signal.signal(signal.SIGTERM, self.__signal_handler)
        signal.signal(signal.SIGINT, self.__signal_handler)

    def clients_connections(self):
        processor_clients = []
        while self.serverIsAlive:
            try:
                client_sock = self.__accept_new_connection()
                if client_sock:
                    client_process = multiprocessing.Process(target=self.__handle_client_connection, args=(client_sock,))
                    processor_clients.append(client_process)
                    client_process.daemon = True
                    client_process.start()
                    client_sock.close()
            except OSError as e:
                logging.error(f"action: accept_connections | result: fail | error: {e}")
                break

        for client in processor_clients:
            client.join()

        self.__close_server()

    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """
        self.set_signals()
        report_service = multiprocessing.Process(target=self.__handler_reports)
        report_service.daemon = True
        report_service.start()
        self.clients_connections()
        report_service.join()

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        client_id = None
        try:
            addr = client_sock.getpeername()
            logging.info(f"action: handle_client_connection | result: start | addr: {addr}")
            
            while True:
                dto_message = self.receive_message(client_sock)
                if not dto_message:
                    logging.warning(f"action: handle_client_connection | result: fail | error: invalid message received")
                    break
                    
                client_id = dto_message.id_client
                with self.clients_lock:
                    if client_id not in self.clients:
                        logging.info(f"action: add_client | result: success | client_id: {client_id}")
                        self.clients[client_id] = client_sock

                if dto_message.type_message == ClientCommunication.TYPE_FINISH_COMMUNICATION:
                    logging.info(f"action: client_finish | result: success | client_id: {client_id}")
                    break
                    
                self.handle_client_connection(client_sock, dto_message)
                
        except OSError as e:
            logging.error(f"action: handle_client_connection | result: fail | error: {e}")
        finally:
            # Ensure client is removed from dictionary when connection closes
            if client_id is not None:
                with self.clients_lock:
                    if client_id in self.clients:
                        logging.info(f"action: remove_client | result: success | client_id: {client_id}")
                        del self.clients[client_id]
            
            client_sock.close()
            logging.info(f"action: handle_client_connection | result: socket_closed | addr: {addr if 'addr' in locals() else 'unknown'}")

    def __handler_reports(self):
        self.rabbit_mq_report_connection = RabbitMQConnectionHandler(
            producer_exchange_name=None,
            producer_queues_to_bind=None,
            consumer_exchange_name=self.consumer_exchange_name,
            consumer_queues_to_recv_from=[self.consumer_queue]
        )
        self.rabbit_mq_report_connection.set_message_consumer_callback(self.consumer_queue, self.callback)
        self.rabbit_mq_report_connection.start_consuming()

    def callback(self, ch, method, properties, body):
        data = MiddlewareMessage.decode_from_bytes(body)
        lines = data.get_batch_iter_from_payload()
        result_query = []
        if data.type == MiddlewareMessageType.RESULT_Q1:
            #logging.info(f"action: receive_response_query_1 | result: success | code: {data.type}")
            for line in lines:
                logging.info(f"action: response_query_1 | client: {data.client_id} | line: {line}")
                result_query.append(line)
            self.send_result_query(MiddlewareMessage.write_csv_batch(result_query), ClientCommunication.RESULT_QUERY_1 , data.client_id)
        elif data.type == MiddlewareMessageType.RESULT_Q2:            
            for line in lines:
                logging.info(f"action: response_query_2 | client: {data.client_id} | line: {line}")
                result_query.append(line)
            self.send_result_query(MiddlewareMessage.write_csv_batch(result_query), ClientCommunication.RESULT_QUERY_2 , data.client_id)
        elif data.type == MiddlewareMessageType.RESULT_Q3:            
            for line in lines:
                logging.info(f"action: response_query_3 | client: {data.client_id} | line: {line}")
                result_query.append(line)
            self.send_result_query(MiddlewareMessage.write_csv_batch(result_query), ClientCommunication.RESULT_QUERY_3 , data.client_id)
        elif data.type == MiddlewareMessageType.RESULT_Q4:            
            for line in lines:
                logging.info(f"action: response_query_4 | client: {data.client_id} | line: {line}")
                result_query.append(line)
            self.send_result_query(MiddlewareMessage.write_csv_batch(result_query), ClientCommunication.RESULT_QUERY_4 , data.client_id)
        elif data.type == MiddlewareMessageType.RESULT_Q5:
            for line in lines:
                logging.info(f"action: response_query_5 | client: {data.client_id} | line: {line}")
                result_query.append(line)
            self.send_result_query(MiddlewareMessage.write_csv_batch(result_query), ClientCommunication.RESULT_QUERY_5 , data.client_id)
        elif data.type == MiddlewareMessageType.EOF_RESULT_Q1:
            logging.info(f"EOF_RESULT_Q1")
            self.send_result_query(MiddlewareMessage.write_csv_batch(result_query), ClientCommunication.EOF_QUERY_1 , data.client_id)
        elif data.type == MiddlewareMessageType.EOF_RESULT_Q2:
            logging.info(f"EOF_RESULT_Q2")
            self.send_result_query(MiddlewareMessage.write_csv_batch(result_query), ClientCommunication.EOF_QUERY_2 , data.client_id)
        elif data.type == MiddlewareMessageType.EOF_RESULT_Q3:
            logging.info(f"EOF_RESULT_Q3")
            self.send_result_query(MiddlewareMessage.write_csv_batch(result_query), ClientCommunication.EOF_QUERY_3 , data.client_id)   
        elif data.type == MiddlewareMessageType.EOF_RESULT_Q4:
            logging.info(f"EOF_RESULT_Q4")
            self.send_result_query(MiddlewareMessage.write_csv_batch(result_query), ClientCommunication.EOF_QUERY_4 , data.client_id)
        elif data.type == MiddlewareMessageType.EOF_RESULT_Q5:
            logging.info(f"EOF_RESULT_Q5")
            self.send_result_query(MiddlewareMessage.write_csv_batch(result_query), ClientCommunication.EOF_QUERY_5 , data.client_id)

    def handle_client_connection(self, client_sock, msg_type):
        self.publisher_connection = RabbitMQConnectionHandler(
            producer_exchange_name=self.producer_exchange_name,
            producer_queues_to_bind={
                self.producer_queue_of_movies: [self.producer_queue_of_movies],
                self.producer_queue_of_ratings: [self.producer_queue_of_ratings],
                self.producer_queue_of_credits: [self.producer_queue_of_credits]
            },
            consumer_exchange_name=None,
            consumer_queues_to_recv_from=None
        )
        logging.info(f"action: receive_message | result: success | code: {msg_type.type_message}")
        if msg_type.type_message == ClientCommunication.TYPE_QUERY:
            self.__handle_query(client_sock, msg_type.payload)
        # Agregar las demas querys aqui

    def __handle_query(self, client_sock, query):
        """
        Handle query from client

        Function blocks until a query is received. Then the
        function is executed and the result is sent to the client
        """
        query_number = int(query)
        if query_number == ALL_QUERY:
            self.__handle_all_query(client_sock)
        elif query_number == QUERY_1:
            self.start_query_1()
        elif query_number == 2:
            self.start_query_2()
        elif query_number == 3:
            self.start_query_3()
        elif query_number == 4:
            self.start_query_4()
        elif query_number == 5:
            self.start_query_5()
        

    def __handle_all_query(self, client_sock):
        query_number = ClientCommunication.ALL_QUERYS.value
        while True:
            dto_message = self.receive_message(client_sock)
            if dto_message.id_client not in self.clients_batch_received:
                self.clients_batch_received[dto_message.id_client] = {
                    ClientCommunication.BATCH_MOVIES: 0,
                    ClientCommunication.BATCH_RATINGS: 0,
                    ClientCommunication.BATCH_CREDITS: 0
                }
            if dto_message.type_message == ClientCommunication.BATCH_MOVIES:
                self.receive_file(client_sock, query_number, dto_message, ClientCommunication.EOF_MOVIES) 
            elif dto_message.type_message == ClientCommunication.BATCH_RATINGS:
                self.receive_file(client_sock, query_number, dto_message, ClientCommunication.EOF_RATINGS)
            elif dto_message.type_message == ClientCommunication.BATCH_CREDITS:
                self.receive_file(client_sock, query_number, dto_message, ClientCommunication.EOF_CREDITS)
            elif dto_message.type_message == ClientCommunication.FINISH_SEND_FILES:
                logging.info(f"action: receive_message | result: success | code: {dto_message.type_message}")            
                break

    def receive_file(self, client_sock, query_number, msg, eof_value):
        """
        Receive a file from the client

        Function blocks until a file is received. Then the
        function is saved in the specified path
        """
        message = msg
        # Process the initial message that was passed in first
        while message.type_message != eof_value:
            self.clients_batch_received[message.id_client][message.type_message] += 1
            batchData = message.payload.replace('|', '\n')
            self.send_batch_to_preprocessor(
                batch=batchData,
                type_batch=message.type_message,
                seq_number=self.clients_batch_received[message.id_client][message.type_message],
                query_number=query_number,
                client_id=message.id_client
            )
            # self.send_ack(client_sock, message.id_client, ClientCommunication.TYPE_ACK.value,"Batch received")
            message = self.receive_message(client_sock)   

        self.send_eof_to_preprocessor(message.type_message, query_number, message.id_client)
        return

    def send_ack(self, client_sock, id_client, ack_type, message=None):
        """
        Send ack to the client

        Function blocks until the ack is sent. Then the
        function returns True if the ack was sent successfully
        or False if there was an error
        """
        ack = MessageProtocol(
            idClient=id_client,
            typeMessage=ack_type,
            payload=message
        )
        return self.send_message(client_sock, ack)

    def save_batch_in_file(self, batch, filename):
        """
        Save batch in a file by appending the new data
        """
        with open(filename, 'a') as f:
            for line in batch:
                f.write(line + '\n')

    def receive_message(self, sock) -> MessageProtocol:
        """
        Receive a message from the socket and decode it

        Function blocks until a message is received. Then the
        message is decoded and returned
        """
        header = self.__recv_all(sock, 4)
        if not header:
            logging.error(f"action: receive_message | result: fail | error: short-read")
            return None

        messageSize = int.from_bytes(header, byteorder='big')
        #logging.info(f"action: receive_message | result: success | size: {messageSize}")  
        data = self.__recv_all(sock, messageSize)
        return MessageProtocol.decodeMessageBytes(data)

    def send_message(self, sock, message):
        """
        Send a message to the socket

        Function blocks until the message is sent. Then the
        function returns True if the message was sent successfully
        or False if there was an error
        """
        # Encode message
        encoded_message = message.encodeMessageBytes()
        # Get size of message
        size = len(encoded_message)
        # Send size of message
        self.__send_all(sock, size.to_bytes(4, byteorder='big'))
        # Send message
        return self.__send_all(sock, encoded_message)
    
    
    def __recv_all(self, sock, size):
        data = b''
        while len(data) < size:
            try:
                chunk = sock.recv(size - len(data))
                if not chunk:
                    return None
                data += chunk
            except OSError as e:
                logging.error(f"action: receive_message | result: fail | error: {e}")
                return None
        return data

    def send_batch_to_preprocessor(self, batch, type_batch, seq_number, query_number, client_id):
        batch_type = None
        producer_queue = None
        if type_batch == ClientCommunication.BATCH_MOVIES:
            producer_queue = self.producer_queue_of_movies
            batch_type = MiddlewareMessageType.MOVIES_BATCH
        elif type_batch == ClientCommunication.BATCH_RATINGS:
            producer_queue = self.producer_queue_of_ratings
            batch_type = MiddlewareMessageType.RATINGS_BATCH
        elif type_batch == ClientCommunication.BATCH_CREDITS:
            producer_queue = self.producer_queue_of_credits
            batch_type = MiddlewareMessageType.CREDITS_BATCH

        msg = MiddlewareMessage(
            query_number=query_number,
            client_id=client_id,
            seq_number=seq_number,
            type=batch_type,
            payload=batch
        )

        self.publisher_connection.send_message(
            routing_key=producer_queue,
            msg_body=msg.encode_to_str()
        )

    def send_eof_to_preprocessor(self, type_batch, query_number, client_id):
        typeEof = None
        producer_queue = None
        eof_number = 0
        if type_batch == ClientCommunication.EOF_MOVIES:
            typeEof = MiddlewareMessageType.EOF_MOVIES
            eof_number = self.clients_batch_received[client_id][ClientCommunication.BATCH_MOVIES] + 1 
            producer_queue = self.producer_queue_of_movies
        elif type_batch == ClientCommunication.EOF_RATINGS:
            typeEof = MiddlewareMessageType.EOF_RATINGS
            eof_number = self.clients_batch_received[client_id][ClientCommunication.BATCH_RATINGS] + 1 
            producer_queue = self.producer_queue_of_ratings
        elif type_batch == ClientCommunication.EOF_CREDITS:
            typeEof = MiddlewareMessageType.EOF_CREDITS
            eof_number = self.clients_batch_received[client_id][ClientCommunication.BATCH_CREDITS] + 1
            producer_queue = self.producer_queue_of_credits

        self.publisher_connection.send_message(
            routing_key=producer_queue,
            msg_body=MiddlewareMessage(
                query_number=query_number,
                client_id=client_id,
                seq_number=eof_number,
                type=typeEof,
                payload=""
            ).encode_to_str()
        )

    def send_result_query(self, result_query, type_query, client_id):
        """
        Send result query to the client

        Function blocks until the result query is sent. Then the
        function returns True if the result query was sent successfully
        or False if there was an error
        """
        msg = MessageProtocol(
            idClient=client_id,
            typeMessage=type_query,
            payload=result_query
        )
        with self.clients_lock:
            if client_id in self.clients:
                sock_client = self.clients[client_id]
                self.send_message(sock_client, msg)

    def start_query_1(self, batch):
        return 0

    def start_query_2(self):
        # Buscar del archivo movies_data las columnas:
        return 0

    def start_query_3(self):
        # Buscar del archivo movies_data las columnas:
        return 0

    def start_query_4(self):
        # Buscar del archivo movies_data las columnas:
        return 0

    def start_query_5(self):
        # Buscar del archivo movies_data las columnas:
        return 0


    def __send_all(self, sock, data):
        totalSent = 0
        while totalSent < len(data):
            try:
                sent = sock.send(data[totalSent:])
                if sent == 0:
                    return False
                totalSent += sent
            except OSError as e:
                logging.error(f"action: send_message | result: fail | error: {e}")
                return False
        return True

    def __accept_new_connection(self):
        """
        Accept new connections

        Function blocks until a connection to a client is made.
        Then connection created is printed and returned
        """
        # Connection arrived
        try:
            logging.info('action: accept_connections | result: in_progress')
            c, addr = self._server_socket.accept()
            logging.info(f'action: accept_connections | result: success | ip: {addr[0]}')
            return c
        except OSError:
            return None

    def __signal_handler(self, signum, frame):
        signame = signal.Signals(signum).name
        logging.info(f"action: exit | result: success | signal: {signame}")
        self.serverIsAlive = False
        self.__close_server()

    def __close_server(self):
        self._server_socket.close()
        logging.info("action: close_server | result: success")
        exit(0)