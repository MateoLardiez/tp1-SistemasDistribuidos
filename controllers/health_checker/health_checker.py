import docker
from common.socket_handler import SocketHandler
from common.health_check_message import MessageHealthCheck
from common.defines import HealthCheckMessage
import logging
from multiprocessing import Process, Value
import time
import signal
import hashlib

# Not an env var due to docker pathing within image
CONTROLLERS_NAMES_PATH = '/monitorable_process.txt'
HEALTH_CHECK_PORT = 5000

class HealthChecker():
    def __init__(self, health_check_interval: int, health_check_timeout: int, controller_id: str, num_of_healthcheckers: int):
        self.health_check_interval = health_check_interval
        self.health_check_timeout = health_check_timeout
        self.controller_id = controller_id
        self.num_of_healthcheckers = num_of_healthcheckers
        self.health_checker_id = int(controller_id.split("_")[-1])
        self.joinable_processes = []
        self.serverIsAlive = Value('b', True)

    def set_signals(self):
        signal.signal(signal.SIGTERM, self.__signal_handler)
        signal.signal(signal.SIGINT, self.__signal_handler)

    def __signal_handler(self, signum, frame):
        signame = signal.Signals(signum).name
        logging.info(f"action: exit | result: success | signal: {signame}")
        self.serverIsAlive.value = False
       
    def start(self):
        # Inicializar servidor de health check en un proceso separado
        self.set_signals()
        health_server_process = Process(target=self.__start_health_check)
        health_server_process.start()
        self.__start_health_server()        
        health_server_process.join()  # Esperar a que el proceso de health check termine

    def __start_health_check(self):
        controllers = []
        with open(CONTROLLERS_NAMES_PATH, 'r') as file:
            controllers = file.read().splitlines()
        controllers_to_check = self.__get_controllers_to_check(controllers)
        while self.serverIsAlive.value:
            for controller in controllers_to_check:
                if controller == self.controller_id:
                    continue
                self.__check_controllers_health(controller)
    
    def deterministic_hash(self, value: str) -> int:
        return int(hashlib.md5(value.encode()).hexdigest(), 16)

    def __get_controllers_to_check(self, controllers: list[str]):
        controllers_to_check = []
        for controller in controllers:
            if controller == self.controller_id:
                continue
            if controller.startswith("health_checker") and controller == self.__get_health_checker_to_monitor():
                controllers_to_check.append(controller)
            elif not controller.startswith("health_checker"):
                if controller == "filter_by_year_0":
                # controllers_to_check.append(f"filter_by_year_0") # TODO: SACAR ESTO!!!!!!!!!!!!!!!!!!!!!!!!!!!!
                    hash_val = self.deterministic_hash(controller) % self.num_of_healthcheckers
                    selected_id = hash_val
                    if selected_id == self.health_checker_id:
                        logging.info(f"[HEALTH_CHECKER_{self.health_checker_id}] Controller {controller} is assigned to this health checker")
                        controllers_to_check.append(controller)
        logging.info(f"[HEALTH_CHECKER_{self.health_checker_id}] Controllers to check: {controllers_to_check} (total: {len(controllers_to_check)})")
        return controllers_to_check    
            
    def __get_health_checker_to_monitor(self):
        if self.health_checker_id == self.num_of_healthcheckers - 1:
            return "health_checker_0"
        return f"health_checker_{self.health_checker_id + 1}"    

    def __check_controllers_health(self, controller: str):
        while self.serverIsAlive.value:
            try:
                # Crear nueva instancia de socket handler para cada chequeo
                socket_handler = SocketHandler(server_mode=False)
                socket_handler.create_socket(timeout=self.health_check_timeout)
                
                # Conectar al controlador en el puerto de health check
                socket_handler.connect(controller, HEALTH_CHECK_PORT)
                
                # Crear y enviar mensaje de health check usando bytes directos
                msg_bytes = MessageHealthCheck(HealthCheckMessage.HEALTH_CHECK).encodeMessageBytes()
                sock = socket_handler.get_socket()
                
                sock.send(msg_bytes)
                
                # Recibir respuesta directamente del socket
                response_bytes = MessageHealthCheck.decode_message_bytes(sock.recv(1))  # Solo necesitamos 1 byte para el tipo
                socket_handler.close()
                
                if not response_bytes or response_bytes.health_check_type != HealthCheckMessage.HEALTH_CHECK_ALIVE:
                    # logging.error(f"Controller {controller} is not healthy")
                    raise RuntimeError()
                    
            except Exception as e:
                self.__revive_controller(controller)
            
            time.sleep(self.health_check_interval)
    
    def __revive_controller(self, controller: str):
        try:
            client = docker.APIClient()
            client.start(controller)
            logging.info(f"[HEALTH_CHECKER_{self.health_checker_id}] Controller {controller} has been REVIVED")
        except Exception as e:
            logging.error(f"[HEALTH_CHECKER_{self.health_checker_id}] Failed to revive controller {controller}: {e}")
    
    def __start_health_server(self):
        """
        Inicia el servidor de health check que responde a las consultas de otros health checkers
        """
        server_handler = SocketHandler(server_mode=True)
        if not server_handler.create_socket(port=HEALTH_CHECK_PORT):
            logging.error("Failed to create health check server")
            return
        
        while self.serverIsAlive.value:
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
