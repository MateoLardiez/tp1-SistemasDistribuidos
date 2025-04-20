from common.defines import ClientCommunication

class MessageProtocol:
    def __init__(self, idClient, typeMessage, payload):
        self.id_client = idClient
        self.type_message = ClientCommunication(typeMessage)
        self.payload = payload

    def encodeMessageBytes(self) -> bytes:
        return bytes([self.id_client, self.type_message.value]) + self.payload.encode('utf-8')
    
    @classmethod
    def decodeMessageBytes(cls, messageBytes):
        idClient = messageBytes[0]
        typeMessage = messageBytes[1]
        payload = messageBytes[2:].decode('utf-8')
        return cls(idClient, typeMessage, payload)