package communication

type MessageProtocol struct {
	// IdClient    int
	TypeMessage int
	Payload     []byte
}

func NewMessageProtocol(typeMessage int, payload []byte) *MessageProtocol {
	if payload == nil {
		payload = []byte{}
	}
	return &MessageProtocol{
		// IdClient:    idClient,
		TypeMessage: typeMessage,
		Payload:     payload,
	}
}

func DecodeMessageProtocol(data []byte) *MessageProtocol {
	// idClient := int(data[0])
	typeMessage := int(data[0])
	payload := []byte{}
	if len(data) > 1 {
		payload = data[1:]
	}

	return NewMessageProtocol(typeMessage, payload)
}

func EncodeMessageProtocol(message *MessageProtocol) []byte {
	data := make([]byte, 1+len(message.Payload))
	// data[0] = byte(message.IdClient)
	data[0] = byte(message.TypeMessage)
	if len(message.Payload) > 0 {
		copy(data[1:], message.Payload)
	}
	return data
}
