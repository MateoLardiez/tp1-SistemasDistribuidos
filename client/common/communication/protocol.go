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

func (p *Protocol) SendCode(code int) error {
	// Send the code to the server
	return p.socket.SendAll([]byte{byte(code)}, SIZE_CODE)
}

func (p *Protocol) SendByte(code []byte) error {
	// Send the code to the server
	return p.socket.SendAll(code, SIZE_CODE)
}

func (p *Protocol) SendBatch(code int, data []byte) error {
	// Send the code to the server
	err := p.SendCode(code)
	if err != nil {
		return err
	}

	// Send the size of the data
	sizeData, errSerialize := p.SerializeInt(len(data))
	if errSerialize != nil {
		return errSerialize
	}

	errHeader := p.socket.SendAll(sizeData, SIZE_HEADER)
	if errHeader != nil {
		return errHeader
	}

	// Send the data to the server
	errData := p.socket.SendAll(data, len(data))
	if errData != nil {
		return errData
	}
	return nil
}
