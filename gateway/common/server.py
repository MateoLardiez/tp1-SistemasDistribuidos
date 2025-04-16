import socket
import logging
import signal
from common import utils
import multiprocessing

CODE_AGENCY = b'A'
CODE_BATCH = b'B'
CODE_RESULT = b'R'
CODE_END = b'E'
CODE_WAIT = b'W'
CODE_WINNER = b'S'
SIZE_BATCH = 8 * 1024

class Server:
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
        self.barrier = multiprocessing.Barrier(clients, action=self.__handle_sorteo)

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
            # TODO: Modify the receive to avoid short-reads
            addr = client_sock.getpeername()
            while True:
                code = self.__recv_all(client_sock, 1)
                if code == CODE_END:
                    break
                self.handle_client_connection(client_sock, code)
        except OSError as e:
            logging.error("action: receive_message | result: fail | error: {e}")
        finally:
            client_sock.close()

    def handle_client_connection(self, client_sock, code):
        if code == CODE_BATCH:
            self.__handle_batch(client_sock)
        elif code == CODE_RESULT:
            self.__handle_result(client_sock)

    def recv_id_agency(self, client_sock) -> int:
        idAgency = self.__recv_all(client_sock, 1)
        idAgency = int(idAgency)
        return idAgency

    def __handle_batch(self, client_sock):
        (batch, failed_bets) = self.recv_batch(client_sock)
        if failed_bets > 0:
            logging.error(f"action: apuesta_recibida | result: fail | error: {failed_bets}")
            response = f'FAIL;{len(batch)}'.encode('utf-8')
        else:
            logging.info(f"action: apuesta_recibida | result: success | cantidad: {len(batch)}")
            response = f'SUCCESS;{len(batch)}'.encode('utf-8')
        
        with self.bets_lock:
            utils.store_bets(batch)
        
        response_len = f"{len(response):04d}".encode('utf-8')
        self.__send_all(client_sock, response_len)
        self.__send_all(client_sock, response)

    def __handle_result(self, client_sock):
        idAgency = self.recv_id_agency(client_sock)
        if not idAgency:
            logging.error(f"action: receive_result | result: fail | error: unknown agency")
            return
        
        with self.bets_lock:
            if idAgency not in self.finished_agencies:
                self.finished_agencies.append(idAgency)

        try:
            self.barrier.wait()
            winnersList = self.winners.get(idAgency, [])
            self.__send_winners(client_sock, winnersList, idAgency)
        
        except multiprocessing.BrokenBarrierError:
            self.__send_all(client_sock, CODE_WAIT)

    def __handle_sorteo(self):
        logging.info("action: sorteo | result: success")
        allBets = utils.load_bets()
        winners = {}
        for bet in allBets:
            if utils.has_won(bet):
                if bet.agency not in winners:
                    winners[bet.agency] = []
                winners[bet.agency].append(bet.document)

        for agency, documents in winners.items():
            self.winners[agency] = documents  
    
    def __send_winners(self, client_sock, winnersList, idAgency):
        winnersData = ";".join(map(str, winnersList)).encode("utf-8")
        self.__send_all(client_sock, CODE_WINNER)

        responseLen = f"{len(winnersData):04d}".encode('utf-8')
        self.__send_all(client_sock, responseLen)
        self.__send_all(client_sock, winnersData)

        logging.info(f"action: send_winners | result: success | agency: {idAgency}")

    def recv_batch(self, client_sock) -> tuple[list, int]:
        header = self.__recv_all(client_sock, 4)
        if not header:
            logging.error(f"action: receive_message | result: fail | error: short-read")
            return None, 0
        
        buffer = bytearray()
        messageSize = int(header)
        receivedBytes = 0
        failedBets = 0
        bets = []

        while receivedBytes < messageSize:
            chunk = client_sock.recv(min(SIZE_BATCH, messageSize - receivedBytes))
            if not chunk:
                logging.error(f"action: receive_message | result: fail | error: connection-lost")
                return None, 0
            buffer.extend(chunk)
            receivedBytes += len(chunk)
            
        batchData = buffer.decode('utf-8').strip()
        batchList = batchData.split('\n')

        for batch in batchList:
            batchBets = batch.split('|')
            for b in batchBets:
                bet = b.split(';')
                if len(bet) == 6:
                    bets.append(utils.Bet(*bet))
                else:
                    failedBets += 1
        return bets, failedBets
    
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