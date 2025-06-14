from common.defines import HealthCheckMessage

class MessageHealthCheck:
    def __init__(self, typeMessage):
        self.health_check_type = typeMessage
        
    def encodeMessageBytes(self) -> bytes:
        type_val = self.health_check_type.value if hasattr(self.health_check_type, 'value') else self.health_check_type
        return bytes([type_val])

    @classmethod
    def decode_message_bytes(cls, messageBytes):
        if len(messageBytes) == 0:
            return None
        type_val = messageBytes[0]
        typeMessage = HealthCheckMessage(type_val)
        return cls(typeMessage)