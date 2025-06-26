package communication

import (
	"encoding/binary"
)

type Protocol struct {
	socket *Socket
}

func NewProtocol(address string) (*Protocol, error) {
	socket, err := NewSocket(address)
	if err != nil {
		return nil, err
	}
	return &Protocol{socket: socket}, nil
}

func (p *Protocol) SendAll(data []byte, size int) error {
	err := p.socket.SendAll(data, size)
	if err != nil {
		return err
	}
	return nil
}

func (p *Protocol) ReceiveAll(size int) ([]byte, error) {
	data, err := p.socket.ReceiveAll(size)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func (p *Protocol) Close() error {
	if !p.socket.IsConnected() {
		err := p.socket.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Protocol) SerializeInt(value int) ([]byte, error) {
	buf := make([]byte, 4)
	binary.BigEndian.PutUint32(buf, uint32(value))
	return buf, nil
}

func (p *Protocol) SendSizeMessage(code int) error {
	// Send the size of the data
	sizeData, errSerialize := p.SerializeInt(code)
	if errSerialize != nil {
		return errSerialize
	}

	errHeader := p.socket.SendAll(sizeData, SIZE_HEADER)
	if errHeader != nil {
		return errHeader
	}
	return nil
}

func (p *Protocol) ReceiveSizeMessage() (int, error) {
	// Receive the size of the data
	sizeData, errHeader := p.socket.ReceiveAll(SIZE_HEADER)
	if errHeader != nil {
		return 0, errHeader
	}
	size := binary.BigEndian.Uint32(sizeData)
	return int(size), nil
}

func (p *Protocol) SendMessage(message *MessageProtocol) error {
	// Send the code to the server
	messageEncode := EncodeMessageProtocol(message)

	err := p.SendSizeMessage(len(messageEncode))
	if err != nil {
		return err
	}
	// Send the size of the data
	errSize := p.SendAll(messageEncode, len(messageEncode))
	if errSize != nil {
		return errSize
	}

	return nil
}

func (p *Protocol) ReceiveMessage() (*MessageProtocol, error) {
	// Receive the code from the server
	sizeData, errCode := p.ReceiveSizeMessage()
	if errCode != nil {
		return nil, errCode
	}

	// Receive the size of the data
	data, errHeader := p.socket.ReceiveAll(int(sizeData))
	if errHeader != nil {
		return nil, errHeader
	}

	message := DecodeMessageProtocol(data)
	// Receive the data from the server
	return message, nil
}
