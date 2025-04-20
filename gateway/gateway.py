import socket
import logging
import signal
# from common import utils
import multiprocessing
import pika
from common.middleware_message_protocol import MiddlewareMessage, MiddlewareMessageType

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
        self.max_agencies = clients

        self.manager = multiprocessing.Manager()
        self.winners = self.manager.dict()
        self.finished_agencies = self.manager.list()
        self.bets_lock = multiprocessing.Lock()

        # Initialize RabbitMQ connection
        self.rabbitmq_connection = pika.BlockingConnection(pika.ConnectionParameters(host='rabbitmq'))
        # Canal para consumir mensajes (resultados)
        self.consumer_channel = self.rabbitmq_connection.channel() 
        # Canal para publicar mensajes (consultas)
        self.publisher_channel = self.rabbitmq_connection.channel()

    def run(self):
        """
        Dummy Server loop

        Server that accept a new connections and establishes a
        communication with a client. After client with communucation
        finishes, servers starts to accept new connections again
        """

        # TODO: Modify this program to handle signal to graceful shutdown
        # the server

        signal.signal(signal.SIGTERM, self.__signal_handler)
        signal.signal(signal.SIGINT, self.__signal_handler)

        # Handler queue
        process_queue = multiprocessing.Process(target=self.__handler_results_queue)
        process_queue.daemon = True
        process_queue.start()

        while self.serverIsAlive:
            try:
                client_sock = self.__accept_new_connection()
                if client_sock:
                    client_process = multiprocessing.Process(target=self.__handle_client_connection, args=(client_sock,))
                    client_process.daemon = True
                    client_process.start()
                    client_sock.close()
            except OSError as e:
                logging.error(f"action: accept_connections | result: fail | error: {e}")
                break

        self.__close_server()

    def __handle_client_connection(self, client_sock):
        """
        Read message from a specific client socket and closes the socket

        If a problem arises in the communication with the client, the
        client socket will also be closed
        """
        try:
            addr = client_sock.getpeername()
            while True:
                code = self.__recv_all(client_sock, 1)
                msg_type = int.from_bytes(code, byteorder='big')
                if msg_type == CODE_END:
                    break
                self.handle_client_connection(client_sock, msg_type)
        except OSError as e:
            logging.error(f"action: receive_message | result: fail | error: {e}")
        finally:
            client_sock.close()

    def __handler_results_queue(self):
        self.consumer_channel.exchange_declare(exchange='results', exchange_type='direct')
        result = self.consumer_channel.queue_declare(queue='results')
        queue_name = result.method.queue
        self.consumer_channel.queue_bind(exchange='results', queue=queue_name, routing_key='filter_by_country_result')
        self.consumer_channel.basic_consume(queue=queue_name, on_message_callback=self.callback, auto_ack=True)
        self.consumer_channel.start_consuming()

    def callback(self, ch, method, properties, body):
        data = body.decode('utf-8')
        logging.info(f"action: receive_filter_by_country | result: success | Data filtrada: {data}")

    def handle_client_connection(self, client_sock, msg_type):
        if msg_type == CODE_ALL_QUERYS:
            logging.info(f"action: receive_message | result: success | code: {msg_type}")
            self.__handle_all_query(client_sock)
        # Agregar las demas querys aqui

    def __handle_all_query(self, client_sock):
        # Create empty files at the beginning
        open('movies.csv', 'w').close()
        open('ratings.csv', 'w').close()
        open('credits.csv', 'w').close()
        
        while True:
            code = self.__recv_all(client_sock, 1)
            msg_type = int.from_bytes(code, byteorder='big')
            logging.info(f"action: receive_message | result: success | code: {msg_type}")
            if msg_type == BATCH_MOVIES:
                batch = self.__handle_batch(client_sock)
                self.save_batch_in_file(batch, 'movies.csv')
                self.start_query_1(batch)                
            elif msg_type == BATCH_RATINGS:
                batch = self.__handle_batch(client_sock)
                self.save_batch_in_file(batch, 'ratings.csv')
            elif msg_type == BATCH_CREDITS:
                batch = self.__handle_batch(client_sock)
                self.save_batch_in_file(batch, 'credits.csv')
            elif msg_type == BATCH_END:
                break
        logging.info(f"action: finished_receiving_files | result: success")

    def save_batch_in_file(self, batch, filename):
        """
        Save batch in a file by appending the new data
        """
        with open(filename, 'a') as f:
            for line in batch:
                f.write(line + '\n')

    def __handle_batch(self, client_sock) -> str:
        batch = self.recv_batch(client_sock)
        logging.info(f"action: apuesta_recibida | result: success | cantidad: {len(batch)}")        
        response = f'SUCCESS;{len(batch)}'.encode('utf-8')
        response_len = f"{len(response):04d}".encode('utf-8')
        self.__send_all(client_sock, response_len)
        self.__send_all(client_sock, response)
        return batch

    def recv_batch(self, client_sock) -> str:
        header = self.__recv_all(client_sock, 4)
        if not header:
            logging.error(f"action: receive_message | result: fail | error: short-read")
            return None, 0
        
        buffer = bytearray()
        messageSize = int.from_bytes(header, byteorder='big')
        logging.info(f"action: receive_message | result: success | size: {messageSize}")
        receivedBytes = 0

        while receivedBytes < messageSize:
            chunk = client_sock.recv(min(SIZE_BATCH, messageSize - receivedBytes))
            if not chunk:
                logging.error(f"action: receive_message | result: fail | error: connection-lost")
                return None, 0
            buffer.extend(chunk)
            receivedBytes += len(chunk)
            
        batchData = buffer.decode('utf-8').strip()
        batchList = batchData.split('\n')[0]
        batchList = batchList.replace('|', '\n')

        # for batch in batchList:
        #     batchFile = batch.split('|')

        return batchList
    
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


    def start_query_1(self, batch):
        self.publisher_channel.exchange_declare(exchange='movies', exchange_type='direct')
        msg = MiddlewareMessage(
            query_number=1,
            client_id=1,
            type=MiddlewareMessageType.MOVIES_BATCH,
            payload=batch)
        # Enviar la línea al filtro
        self.publisher_channel.basic_publish(exchange='movies', routing_key="filter_by_country", body=msg.encode_to_str())
        # logging.info(f"action: send_RabbitMq_message | result: success | message: {line}")

        
        # self.rabbitmq_connection.close()
        # Buscar del archivo movies_data las columnas: ["id", "title", "production_countries", "release_date", "genres"]
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