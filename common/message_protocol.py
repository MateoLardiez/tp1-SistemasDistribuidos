from common.defines import ClientCommunication

class MessageProtocol:
    def __init__(self, typeMessage, payload=None):
        # self.id_client = idClient
        self.type_message = ClientCommunication(typeMessage)
        self.payload = payload

    def encodeMessageBytes(self) -> bytes:
        if self.payload is None:
            return bytes([self.type_message.value])
        else:
            return bytes([self.type_message.value]) + self.payload.encode('utf-8')
    
    @classmethod
    def decodeMessageBytes(cls, messageBytes):
        # idClient = messageBytes[0]
        typeMessage = messageBytes[0]
        payload = messageBytes[1:].decode('utf-8')
        return cls(typeMessage, payload)