from killer import Killer
import logging
import os
import sys

class ColorFormatter(logging.Formatter):
    """Logging Formatter to add colors to levelname only"""
    
    # ANSI color codes
    cyan = "\x1b[36m"
    reset = "\x1b[0m"
    
    def format(self, record):
        # Create a copy of the record to avoid modifying the original
        record_copy = logging.makeLogRecord(record.__dict__)
        # Add cyan color only to the levelname
        record_copy.levelname = f"{self.cyan}{record.levelname}{self.reset}"
        # Use the standard format
        formatter = logging.Formatter("| %(levelname)s | %(message)s")
        return formatter.format(record_copy)

def initialize_log(logging_level):
    """
    Python custom logging initialization with colors

    Current timestamp is added to be able to identify in docker
    compose logs the date when the log has arrived
    """
    # Remove existing handlers
    for handler in logging.root.handlers[:]:
        logging.root.removeHandler(handler)
    
    # Create console handler with color formatter
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(ColorFormatter())
    
    # Configure root logger
    logging.basicConfig(
        level=logging_level,
        handlers=[console_handler]
    )
    
def main():
    initialize_log("INFO")
    kill_percentage = int(os.getenv("KILL_PERCENTAGE"))
    interval = int(os.getenv("INTERVAL"))
    n_health_checkers = int(os.getenv("N_HEALTHCHECKERS"))
    killer = Killer(
        interval=interval, 
        kill_percentage=kill_percentage,
        n_health_checkers=n_health_checkers
    )

    # Verificar si se pasa un argumento para modo interactivo
    if len(sys.argv) > 1 and sys.argv[1] == "--interactive":
        killer.start_interactive_mode()
    elif len(sys.argv) > 1 and sys.argv[1] == "--kill":
        # Modo para matar un contenedor específico
        if len(sys.argv) > 2:
            container_name = sys.argv[2]
            success = killer.kill_container_by_name(container_name)
            if success:
                logging.info(f"Container '{container_name}' killed successfully")
            else:
                logging.error(f"Failed to kill container '{container_name}'")
                sys.exit(1)
        else:
            logging.error("Usage: python main.py --kill <container_name>")
            sys.exit(1)
    else:
        # Modo original
        killer.start()
    
if __name__ == "__main__":
    main()