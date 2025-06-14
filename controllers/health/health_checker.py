import docker
import docker.models
import docker.models.containers
import docker.types
# from shared.protocol_messages import SystemMessage, SystemMessageType
# from shared.socket_connection_handler import SocketConnectionHandler
import logging
from multiprocessing import Process
import time
# from shared.monitorable_process import MonitorableProcess

# Not an env var due to docker pathing within image
CONTROLLERS_NAMES_PATH = '/monitorable_controllers.txt'
HEALTH_CHECK_PORT = 5000

class HealthChecker(MonitorableProcess):
    def __init__(self, health_check_interval: int, health_check_timeout: int, controller_name: str, num_of_healthcheckers: int):
        super().__init__(controller_name)
        self.health_check_interval = health_check_interval
        self.health_check_timeout = health_check_timeout
        self.controller_name = controller_name
        self.socket_connection_handler = None
        self.num_of_healthcheckers = num_of_healthcheckers
        self.health_checker_id = int(controller_name.split("_")[-1])
       
    def start(self):
        controllers = []
        with open(CONTROLLERS_NAMES_PATH, 'r') as file:
            controllers = file.read().splitlines()
        controllers_to_check = self.__filter_controllers(controllers)
        for controller in controllers_to_check:
            if controller == self.controller_name: 
                continue
            p = Process(target=self.__check_controllers_health, args=(controller,))
            self.joinable_processes.append(p)
            p.start()
    
    def __filter_controllers(self, controllers: list[str]):
        """
        Since there is self.num_of_healthcheckers healthcheckers, we need to split the controllers list into self.num_of_healthcheckers parts
        and only add one health_checker to check, that need to be a circular relation. For example, with 3 health_checkers: health_checker_1 -> health_checker_2 -> health_checker_3 -> health_checker_1.
        
        For controllers that are not healthcheckers, they will be separeted so only a one health_checker will check them.

        """
        
        controllers_to_check = []
        for controller in controllers:
            if controller == self.controller_name:
                continue
            if controller.startswith("health_checker") and controller == self.__get_health_checker_to_monitor():
                controllers_to_check.append(controller)
            elif not controller.startswith("health_checker"):
                hash_val = hash(controller) % self.num_of_healthcheckers
                selected_id = hash_val + 1
                if selected_id == self.health_checker_id:
                    controllers_to_check.append(controller)
        logging.info(f"[HEALTH_CHECKER_{self.health_checker_id}] Controllers to check: {controllers_to_check} (total: {len(controllers_to_check)})")  
        return controllers_to_check
            
                
            
    def __get_health_checker_to_monitor(self):
        if self.health_checker_id == self.num_of_healthcheckers:
            return "health_checker_1"
        return f"health_checker_{self.health_checker_id + 1}"    

    def __check_controllers_health(self, controller: str):
        while True:
            try:
                logging.debug(f"Connecting to controller {controller}")
                self.socket_connection_handler = SocketConnectionHandler.connect_and_create(controller, HEALTH_CHECK_PORT, self.health_check_timeout)
                msg = SystemMessage(msg_type=SystemMessageType.HEALTH_CHECK, client_id=0, controller_name=self.controller_name, controller_seq_num=0)
                self.socket_connection_handler.send_message(msg.encode_to_str())

                response_msg = SystemMessage.decode_from_bytes(self.socket_connection_handler.read_message_raw())
                if response_msg.type != SystemMessageType.ALIVE:
                    logging.error(f"Controller {controller} is not healthy")
                    self.__revive_controller(controller)
                else:
                    logging.debug(f"Controller {controller} is healthy")
            except Exception as e:
                logging.error(f"Failed to connect to controller {controller}: {str(e)}")
                self.__revive_controller(controller)
            
            time.sleep(self.health_check_interval)
    
    def __revive_controller(self, controller: str):
        docker.APIClient().start(controller)
        logging.info(f"[HEALTH_CHECKER_{self.health_checker_id}] Controller {controller} has been REVIVED")
        
        
        