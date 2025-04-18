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

func (p *Protocol) Serialize(data string) ([]byte, error) {
	// Convert the string data to bytes
	dataBytes := []byte(data)

	// Create buffer with size header (4 bytes) + data
	buf := make([]byte, len(dataBytes))

	// Write the length of the data in big-endian format in the first 4 bytes
	binary.BigEndian.PutUint32(buf, uint32(len(dataBytes)))

	// Copy the data after the header
	// copy(buf[SIZE_HEADER:], dataBytes)

	return buf, nil
}

func (p *Protocol) SendCodeQuery(code int) error {
	// Send the code to the server
	return p.socket.SendAll([]byte{byte(code)}, SIZE_CODE)
}
