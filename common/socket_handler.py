import socket
import logging
from common.message_protocol import MessageProtocol
from typing import Optional, Tuple, Union

class SocketHandler:
    """
    Clase que maneja las comunicaciones a través de sockets.
    Proporciona métodos para crear, conectar, enviar y recibir mensajes utilizando el protocolo MessageProtocol.
    """
    
    def __init__(self, server_mode: bool = False):
        """
        Inicializa un nuevo manejador de sockets
        
        Args:
            server_mode: Indica si el socket se usará como servidor (True) o como cliente (False)
        """
        self._socket = None
        self._server_mode = server_mode
        self._connected = False
        self.timeout = None
        
    def create_socket(self, port: int = 0, ip: str = '', listen_backlog: int = 5, timeout=None) -> bool:
        """
        Crea un socket y lo configura según el modo (servidor o cliente)
        
        Args:
            port: Puerto donde escuchar conexiones (modo servidor) o al cual conectarse (modo cliente)
            ip: Dirección IP donde escuchar (modo servidor) o a la cual conectarse (modo cliente)
            listen_backlog: Número máximo de conexiones en cola para aceptar (solo en modo servidor)
            
        Returns:
            True si el socket fue creado exitosamente, False en caso contrario
        """
        try:
            self._socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self._socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            if timeout:
                self._socket.settimeout(timeout)
                self.timeout = timeout
            if self._server_mode:
                self._socket.bind((ip, port))
                self._socket.listen(listen_backlog)
                logging.info(f"action: create_socket | result: success | mode: server | ip: {ip} | port: {port}")
                self._connected = True
                return True
            return True
        except Exception as e:
            logging.error(f"action: create_socket | result: fail | error: {e}")
            return False
    
    def connect(self, ip: str, port: int) -> bool:
        """
        Conecta a un servidor (solo en modo cliente)
        
        Args:
            ip: Dirección IP del servidor
            port: Puerto del servidor
            
        Returns:
            True si la conexión fue establecida exitosamente, False en caso contrario
        """
        if self._server_mode:
            logging.error("action: connect | result: fail | error: Cannot connect in server mode")
            return False
        
        try:
            self._socket.connect((ip, port))
            logging.info(f"action: connect | result: success | ip: {ip} | port: {port}")
            self._connected = True
            return True
        except Exception as e:
            logging.error(f"action: connect | result: fail | error: {e}")
            return False
    
    def accept_connection(self) -> Tuple[Optional['SocketHandler'], Optional[Tuple[str, int]]]:
        """
        Acepta una conexión entrante (solo en modo servidor)
        
        Returns:
            Tupla con un nuevo SocketHandler para la conexión aceptada y la dirección del cliente,
            o (None, None) si ocurrió un error
        """
        if not self._server_mode:
            logging.error("action: accept_connection | result: fail | error: Cannot accept connections in client mode")
            return None, None
        
        try:
            client_sock, client_addr = self._socket.accept()
            client_handler = SocketHandler()
            client_handler._socket = client_sock
            client_handler._connected = True
            logging.info(f"action: accept_connection | result: success | client: {client_addr}")
            return client_handler, client_addr
        except Exception as e:
            logging.error(f"action: accept_connection | result: fail | error: {e}")
            return None, None
    
    def close(self):
        """
        Cierra el socket
        """
        if self._socket and self._connected:
            self._socket.close()
            self._connected = False
            # logging.info("action: close_socket | result: success")
    
    def receive_message(self) -> Optional[MessageProtocol]:
        """
        Recibe un mensaje desde el socket y lo decodifica

        La función se bloquea hasta que se reciba un mensaje. Luego,
        el mensaje es decodificado y retornado
        
        Returns:
            Objeto MessageProtocol decodificado o None si ocurrió un error
        """
        if not self._socket or not self._connected:
            logging.error("action: receive_message | result: fail | error: Socket not connected")
            return None
            
        return SocketHandler.receive_message_from(self._socket)
    
    @staticmethod
    def receive_message_from(sock) -> Optional[MessageProtocol]:
        """
        Recibe un mensaje desde un socket específico y lo decodifica

        La función se bloquea hasta que se reciba un mensaje. Luego,
        el mensaje es decodificado y retornado
        
        Args:
            sock: Socket desde el cual se recibirá el mensaje
            
        Returns:
            Objeto MessageProtocol decodificado o None si ocurrió un error
        """
        header = SocketHandler.__recv_all(sock, 4)
        if not header:
            logging.error(f"action: receive_message | result: fail | error: short-read")
            return None

        messageSize = int.from_bytes(header, byteorder='big')
        #logging.info(f"action: receive_message | result: success | size: {messageSize}")  
        data = SocketHandler.__recv_all(sock, messageSize)
        if not data:
            return None
        return MessageProtocol.decodeMessageBytes(data)

    def send_message(self, message) -> bool:
        """
        Envía un mensaje al socket

        La función se bloquea hasta que el mensaje sea enviado. Luego,
        la función retorna True si el mensaje fue enviado exitosamente
        o False si ocurrió un error
        
        Args:
            message: Objeto MessageProtocol a enviar
            
        Returns:
            True si el mensaje fue enviado exitosamente, False en caso contrario
        """
        if not self._socket or not self._connected:
            logging.error("action: send_message | result: fail | error: Socket not connected")
            return False
            
        return SocketHandler.send_message_to(self._socket, message)
    
    @staticmethod
    def send_message_to(sock, message) -> bool:
        """
        Envía un mensaje a un socket específico

        La función se bloquea hasta que el mensaje sea enviado. Luego,
        la función retorna True si el mensaje fue enviado exitosamente
        o False si ocurrió un error
        
        Args:
            sock: Socket al cual se enviará el mensaje
            message: Objeto MessageProtocol a enviar
            
        Returns:
            True si el mensaje fue enviado exitosamente, False en caso contrario
        """
        # Encode message
        encoded_message = message.encodeMessageBytes()
        # Get size of message
        size = len(encoded_message)
        # Send size of message
        if not SocketHandler.__send_all(sock, size.to_bytes(4, byteorder='big')):
            return False
        # Send message
        return SocketHandler.__send_all(sock, encoded_message)
    
    def get_socket(self) -> socket.socket:
        """
        Obtiene el socket subyacente
        
        Returns:
            El objeto socket
        """
        return self._socket
        
    def is_connected(self) -> bool:
        """
        Verifica si el socket está conectado
        
        Returns:
            True si el socket está conectado, False en caso contrario
        """
        return self._connected
    
    def get_peer_name(self) -> Optional[Tuple[str, int]]:
        """
        Obtiene la dirección del peer conectado
        
        Returns:
            Tupla (ip, puerto) del peer o None si no está conectado
        """
        if not self._socket or not self._connected:
            return None
        try:
            return self._socket.getpeername()
        except:
            return None
    
    def get_sock_name(self) -> Optional[Tuple[str, int]]:
        """
        Obtiene la dirección local del socket
        
        Returns:
            Tupla (ip, puerto) local o None si no está conectado
        """
        if not self._socket or not self._connected:
            return None
        try:
            return self._socket.getsockname()
        except:
            return None
            
    @staticmethod
    def __recv_all(sock, size):
        """
        Recibe exactamente 'size' bytes desde el socket
        
        Args:
            sock: Socket desde el cual recibir datos
            size: Cantidad exacta de bytes a recibir
            
        Returns:
            Los datos recibidos o None si ocurrió un error
        """
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
    
    @staticmethod
    def __send_all(sock, data):
        """
        Envía todos los datos al socket
        
        Args:
            sock: Socket al cual enviar datos
            data: Datos a enviar
            
        Returns:
            True si todos los datos fueron enviados exitosamente, False en caso contrario
        """
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

    
