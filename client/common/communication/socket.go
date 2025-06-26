package communication

import (
	"errors"
	"net"
)

type Socket struct {
	Conn net.Conn
}

func NewSocket(address string) (*Socket, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}
	return &Socket{Conn: conn}, nil
}

func (s *Socket) CreateSocket(address string) error {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return err
	}
	s.Conn = conn
	return nil
}

func (s *Socket) SendAll(data []byte, size int) error {
	totalSend := 0
	for totalSend < size {
		n, err := s.Conn.Write(data[totalSend:])
		if err != nil {
			return err
		}
		totalSend += n
	}
	return nil
}

func (s *Socket) ReceiveAll(size int) ([]byte, error) {
	data := make([]byte, size)
	totalReceived := 0
	for totalReceived < size {
		n, err := s.Conn.Read(data[totalReceived:])
		if err != nil {
			return nil, err
		}
		totalReceived += n
	}
	return data, nil
}

func (s *Socket) Close() error {
	if s.Conn == nil {
		return errors.New("socket is already closed")
	}
	err := s.Conn.Close()
	s.Conn = nil
	return err
}

func (s *Socket) IsConnected() bool {
	return s.Conn != nil
}
