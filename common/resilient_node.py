from common.middleware_connection_handler import RabbitMQConnectionHandler
from common.socket_handler import SocketHandler
from common.health_check_message import MessageHealthCheck
from common.defines import HealthCheckMessage
import logging
import time
import signal
from typing import Optional, List
from multiprocessing import Process, Value
from common.file_manager import FileManager
import json

HEALTH_CHECK_PORT = 5000

class ResilientNode:

    def __init__(self):
        self.rabbitmq_connection_handler: Optional[RabbitMQConnectionHandler] = None
        self.node_is_alive = Value('b', True)
        self.joinable_processes: List[Process] = []
        check_health_process = Process(target=self.__start_health_connection)
        check_health_process.start()
        self.joinable_processes.append(check_health_process)
        self.controller_name = ""
        self.clients_state = {}  # Diccionario para almacenar el estado local de los clientes
        self.set_signals()

    def load_state(self, check_files_state = None):
        self.clients_state = FileManager.load_state(f".data/{self.controller_name}_state.json")
        if check_files_state and self.clients_state != {}:
            check_files_state()  # Función para verificar el estado de los archivos temporales
        # iterar sobre el id del cliente para verificar los archivos temporales

    def check_file(self, client_id, file_type):
        """Check if the file exists for the given client and type"""
        prev_hash = self.clients_state[client_id]["hash_file"][file_type]
        filename = f".data/{file_type}-client-{client_id}"
        new_hash = FileManager.get_file_hash(filename)
        return prev_hash != new_hash

    def update_duplicate_state(self, client_id, file_type, controller_name, seq_number):
        self.clients_state[client_id]["duplicated_batch"][file_type] = False
        self.clients_state[client_id][controller_name] = seq_number
        self.clients_state[client_id]["last_seq_number"] += 1
        filename = f".data/{file_type}-client-{client_id}"
        self.clients_state[client_id]["hash_file"][file_type] = FileManager.get_file_hash(filename)
        self.save_state()

    def save_state(self):
        state = FileManager(f".data/{self.controller_name}_state.json")
        state.save_state(json.dumps(self.clients_state, indent=2))

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