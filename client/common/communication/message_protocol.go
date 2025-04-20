package communication

type MessageProtocol struct {
	idClient    int
	typeMessage int
	payload     []byte
}

func NewMessageProtocol(idClient int, typeMessage int, payload []byte) *MessageProtocol {
	if payload == nil {
		payload = []byte{}
	}
	return &MessageProtocol{
		idClient:    idClient,
		typeMessage: typeMessage,
		payload:     payload,
	}
}

func DecodeMessageProtocol(data []byte) *MessageProtocol {
	idClient := int(data[0])
	typeMessage := int(data[1])
	payload := data[2:]

	return NewMessageProtocol(idClient, typeMessage, payload)
}

func EncodeMessageProtocol(message *MessageProtocol) []byte {
	data := make([]byte, 2+len(message.payload))
	data[0] = byte(message.idClient)
	data[1] = byte(message.typeMessage)
	if len(message.payload) > 0 {
		copy(data[2:], message.payload)
	}
	return data
}
