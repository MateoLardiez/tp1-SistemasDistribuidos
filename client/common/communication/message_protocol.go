package communication

type MessageProtocol struct {
	IdClient    int
	TypeMessage int
	Payload     []byte
}

func NewMessageProtocol(idClient int, typeMessage int, payload []byte) *MessageProtocol {
	if payload == nil {
		payload = []byte{}
	}
	return &MessageProtocol{
		IdClient:    idClient,
		TypeMessage: typeMessage,
		Payload:     payload,
	}
}

func DecodeMessageProtocol(data []byte) *MessageProtocol {
	idClient := int(data[0])
	typeMessage := int(data[1])
	payload := []byte{}
	if len(data) > 2 {
		payload = data[2:]
	}

	return NewMessageProtocol(idClient, typeMessage, payload)
}

func EncodeMessageProtocol(message *MessageProtocol) []byte {
	data := make([]byte, 2+len(message.Payload))
	data[0] = byte(message.IdClient)
	data[1] = byte(message.TypeMessage)
	if len(message.Payload) > 0 {
		copy(data[2:], message.Payload)
	}
	return data
}
