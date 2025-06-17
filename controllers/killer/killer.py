import signal
import docker
import random
import time
import logging


class Killer:
    def __init__(self):
        # self.interval = interval
        # self.kill_percentage = kill_percentage
        # self.max_health_checkers_to_kill = num_of_healthcheckers - 1
        self.safe_nodes = ["health_checker_1", "rabbit", "gateway", "client", "resutester", ""]
        try:
            self.docker_client = docker.APIClient()
            # Verificar que Docker está disponible
            self.docker_client.ping()
            logging.info("Docker connection established successfully")
        except Exception as e:
            logging.error(f"Failed to connect to Docker: {str(e)}")
            logging.error("Make sure Docker socket is mounted: -v /var/run/docker.sock:/var/run/docker.sock")
            raise
        signal.signal(signal.SIGTERM, self.__handle_shutdown)
        
    def __handle_shutdown(self, signum, frame):
        logging.info("Shutting down killer")
        

    def kill_container_by_name(self, container_name):
        """
        Mata un contenedor específico por su nombre
        """
        try:
            logging.info(f"Attempting to kill container: {container_name}")
            self.docker_client.kill(container=container_name, signal='SIGTERM')
            logging.info(f"Successfully killed container: {container_name}")
            return True
        except Exception as e:
            logging.error(f"Failed to kill container {container_name}: {str(e)}")
            return False
    
    def list_running_containers(self):
        """
        Lista todos los contenedores en ejecución
        """
        try:
            containers = self.docker_client.containers(all=False)
            return [(c['Names'][0].lstrip('/'), c['Id'][:12]) for c in containers]
        except Exception as e:
            logging.error(f"Failed to list containers: {str(e)}")
            return []
    
    def start_interactive_mode(self):
        """
        Modo interactivo para recibir comandos por consola.
        Para matar un contenedor, se debe usar: kill <nombre_container>
        """
        logging.info("Interactive mode started. Type 'exit' to quit.")
        logging.info("Interactive Killer Mode")
        logging.info("Type 'kill <container_name>' to kill a container, 'list' to see running containers, or 'exit' to quit.")
        
        while True:
            try:
                command = input("\nEnter command: ").strip()
                
                if command.lower() == 'exit':
                    logging.info("Exiting interactive mode")
                    break
                elif command.lower() == 'list':
                    containers = self.list_running_containers()
                    if containers:
                        logging.info("\nRunning containers:")
                        for name, container_id in containers:
                            logging.info(f"  - {name} ({container_id})")
                    else:
                        logging.warning("No running containers found or failed to list containers")
                    continue

                if command.lower().startswith('kill '):
                    parts = command.split(maxsplit=1)
                    if len(parts) != 2 or not parts[1]:
                        logging.error("Usage: kill <container_name>")
                        continue
                    container_name = parts[1].strip()
                    success = self.kill_container_by_name(container_name)
                    if success:
                        logging.info(f"Container '{container_name}' killed successfully")
                    else:
                        logging.error(f"Failed to kill container '{container_name}'")
                    continue

                logging.warning("Unknown command. Use 'kill <container_name>', 'list', or 'exit'.")


                excluded_nodes = ['health_checker', 'rabbit', 'gateway', 'client', 'results_tester', 'killer']
                if command.lower() == 'fatality':
                    containers = self.list_running_containers()
                    if not containers:
                        logging.warning("No running containers to kill.")
                        continue
                    
                    for name, _ in containers:
                        # Verificar si el nombre NO empieza con ninguno de los prefijos excluidos
                        if not any(name.startswith(prefix) for prefix in excluded_nodes):
                            self.kill_container_by_name(name)

                    logging.info("All running containers have been killed.")
                    
            except KeyboardInterrupt:
                logging.info("Interrupted by user")
                break
            except EOFError:
                logging.info("EOF received, exiting")
                break

    def start(self):
        controllers = []
        
        while True:
            health_checkers_to_kill = 0
            controllers_to_kill = []
            for controller in controllers:
                if random.randint(0, 100) < self.kill_percentage:
                    # do not kill all healthcheckers
                    if controller.startswith("health_checker"):
                        if not controller.startswith("health_checker_1"):
                            if health_checkers_to_kill < self.max_health_checkers_to_kill:
                                health_checkers_to_kill += 1
                                controllers_to_kill.append(controller)
                    else:
                        controllers_to_kill.append(controller)
            time.sleep(self.interval)
     
            logging.info(f"\n \n \t \t <<< KILLING NODES >>>:\n  {controllers_to_kill}\n")
            for controller in controllers_to_kill:
                try: 
                    self.docker_client.kill(container=controller)
                except Exception as e:
                    logging.error(f"Failed to kill controller {controller}: {str(e)}")  

        