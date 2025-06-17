from common.middleware_connection_handler import RabbitMQConnectionHandler
from common.socket_handler import SocketHandler
from common.health_check_message import MessageHealthCheck
from common.defines import HealthCheckMessage
import logging
import time
import signal
from typing import Optional, List
from multiprocessing import Process, Value

HEALTH_CHECK_PORT = 5000

class ResilientNode:

    def __init__(self):
        self.rabbitmq_connection_handler: Optional[RabbitMQConnectionHandler] = None
        self.node_is_alive = Value('b', True)
        self.joinable_processes: List[Process] = []
        check_health_process = Process(target=self.__start_health_connection)
        check_health_process.start()
        self.joinable_processes.append(check_health_process)
        self.set_signals()

    def set_signals(self):
        signal.signal(signal.SIGTERM, self.__signal_handler)
        signal.signal(signal.SIGINT, self.__signal_handler)

    def __signal_handler(self, signum, frame):
        signame = signal.Signals(signum).name
        logging.info(f"action: exit | result: success | signal: {signame}")
        self.node_is_alive.value = False
        if self.rabbitmq_connection_handler:
            logging.info("Closing RabbitMQ connection")
            self.rabbitmq_connection_handler.close_connection()
        for process in self.joinable_processes:
            process.terminate()
            process.join()

    def __start_health_connection(self):
        """
        Inicia el servidor de health check que responde a las consultas de otros health checkers
        """
        server_handler = SocketHandler(server_mode=True)
        if not server_handler.create_socket(port=HEALTH_CHECK_PORT):
            logging.error("Failed to create health check server")
            return
        
        while self.node_is_alive.value:
            try:
                client_handler, _ = server_handler.accept_connection()
                if client_handler:
                    client_sock = client_handler.get_socket()
                    msg_bytes = MessageHealthCheck.decode_message_bytes(client_sock.recv(1))

                    if msg_bytes and msg_bytes.health_check_type == HealthCheckMessage.HEALTH_CHECK:
                        # Responder con HEALTH_CHECK_ALIVE
                        response_bytes = MessageHealthCheck(HealthCheckMessage.HEALTH_CHECK_ALIVE).encodeMessageBytes()
                        client_sock.send(response_bytes)
                    
                    client_handler.close()
            except Exception as e:
                logging.error(f"Error in health server: {e}")
                time.sleep(1)