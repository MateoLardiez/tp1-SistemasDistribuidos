import logging
import sys


class Logger:
    """
    Clase Logger que permite crear y configurar instancias de logger
    para ser reutilizadas en diferentes partes de la aplicación.
    """
    
    def __init__(self, 
                name='GatewayLogger', 
                log_file=None, 
                level=logging.INFO,
                console_output=True):
        """
        Inicializa y configura una instancia de logger.
        
        Args:
            name (str): Nombre del logger
            log_file (str): Ruta del archivo de log (opcional)
            level (int): Nivel de logging (DEBUG, INFO, etc.)
            console_output (bool): Si se debe mostrar output en consola
        """
        self.name = name
        self.log_file = log_file
        self.level = level
        self.console_output = console_output
        self.logger = self._setup_logger()
    
    def _setup_logger(self):
        """
        Configura el logger con el formato y los handlers especificados.
        
        Returns:
            logging.Logger: Instancia de logger configurada
        """
        # Crear logger
        logger = logging.getLogger(self.name)
        logger.setLevel(self.level)
        
        # Formato del log
        formatter = logging.Formatter(
            '%(asctime)s - %(levelname)s - %(message)s',
            datefmt='%H:%M:%S'
        )
        
        # Limpiar handlers existentes
        logger.handlers = []
        
        # Agregar FileHandler si se especifica archivo
        if self.log_file:
            file_handler = logging.FileHandler(
                f"{self.log_file}.log"
            )
            file_handler.setFormatter(formatter)
            logger.addHandler(file_handler)
        
        # Agregar StreamHandler si se requiere output en consola
        if self.console_output:
            console_handler = logging.StreamHandler(sys.stdout)
            console_handler.setFormatter(formatter)
            logger.addHandler(console_handler)
        
        return logger
    
    def debug(self, message):
        """Registra mensaje con nivel DEBUG"""
        self.logger.debug(message)
    
    def info(self, message):
        """Registra mensaje con nivel INFO"""
        self.logger.info(message)
    
    def warning(self, message):
        """Registra mensaje con nivel WARNING"""
        self.logger.warning(message)
    
    def error(self, message):
        """Registra mensaje con nivel ERROR"""
        self.logger.error(message)
    
    def critical(self, message):
        """Registra mensaje con nivel CRITICAL"""
        self.logger.critical(message)
        
    def get_logger(self):
        """
        Retorna la instancia de logger interna.
        
        Returns:
            logging.Logger: La instancia de logger
        """
        return self.logger


# Mantener compatibilidad con código existente
def setup_logger(name='GatewayLogger', 
                log_file=None, 
                level=logging.INFO,
                console_output=True):
    """
    Función de compatibilidad que mantiene el comportamiento anterior.
    Crea y retorna un logger configurado.
    
    Args:
        name (str): Nombre del logger
        log_file (str): Ruta del archivo de log (opcional)
        level (int or str): Nivel de logging (DEBUG, INFO, etc.)
        console_output (bool): Si se debe mostrar output en consola
    
    Returns:
        logging.Logger: Instancia de logger configurada
    """
    # Convert string level to logging constant if needed
    if isinstance(level, str):
        level_map = {
            'DEBUG': logging.DEBUG,
            'INFO': logging.INFO,
            'WARNING': logging.WARNING,
            'ERROR': logging.ERROR,
            'CRITICAL': logging.CRITICAL
        }
        level = level_map.get(level.upper(), logging.INFO)
    
    logger_instance = Logger(name, log_file, level, console_output)
    return logger_instance.get_logger()