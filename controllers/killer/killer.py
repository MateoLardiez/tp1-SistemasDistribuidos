import signal
import docker
import random
import time
import logging

CONTROLLERS_NAMES_PATH = 'monitorable_process.txt'

class Killer:
    def __init__(self, interval, kill_percentage, n_health_checkers):
        self.interval = interval  # Intervalo en segundos entre intentos de matar contenedores
        self.kill_percentage = kill_percentage
        self.max_health_checkers_to_kill = n_health_checkers - 1
        self.safe_nodes = ["health_checker_1", "rabbit", "gateway", "client", "results_tester", ""]
        self.running = True
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
        signal.signal(signal.SIGINT, self.__handle_shutdown)
        
    def __handle_shutdown(self, signum, frame):
        logging.info("Shutting down killer")
        self.running = False

    def kill_container_by_name(self, container_name):
        """
        Mata un contenedor específico por su nombre
        """
        try:
            logging.info(f"Attempting to kill container: {container_name}")
            self.docker_client.kill(container=container_name, signal='SIGKILL')
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


                excluded_nodes = ['health_checker', 'rabbit', 'gateway', 'client', 'results_tester', 'killer', 'spawned_client']
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
        last_killed_time = {}
        cooldown_period = max(self.interval * 4, 10)  # Mínimo 10 segundos o el doble del intervalo
        excluded_prefixes = ['rabbit', 'gateway', 'client', 'results_tester', 'killer']

        with open(CONTROLLERS_NAMES_PATH, 'r') as file:
            controllers = file.read().splitlines()
        
        while self.running:
            # Actualizar la lista de contenedores en ejecución
            running_containers = [c[0] for c in self.list_running_containers()]

            health_checkers_live = [c for c in running_containers if c.startswith("health_checker")]
            
            controller_to_kill = None
            current_time = time.time()
            random_id_to_kill = random.randint(0, len(controllers) - 1) 
            controller = controllers[random_id_to_kill]
            logging.info(f"Selected controller to check: {controller}")

            if controller in running_containers:             
                # Verificar si el contenedor está en período de enfriamiento
                if controller in last_killed_time and current_time - last_killed_time[controller] < cooldown_period:
                    continue
                
                # Verificar si el contenedor es un nodo seguro (no debe ser matado)
                if any(controller.startswith(prefix) for prefix in excluded_prefixes):
                    continue
                
                if controller.startswith("health_checker"):
                    if health_checkers_live.count(controller) <= self.max_health_checkers_to_kill:
                        controller_to_kill = controller
                else:
                    controller_to_kill = controller

            if self.kill_container_by_name(controller_to_kill):
                # Registrar el tiempo en que se mató exitosamente
                last_killed_time[controller_to_kill] = current_time

            time.sleep(self.interval * 4)
     
        