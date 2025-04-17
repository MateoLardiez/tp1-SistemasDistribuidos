package common

import (
	"fmt"
	"io"
	"os"
)

type FileReader struct {
	filePath string
	reader   io.ReadCloser
}

func NewFileReader(filePath string) (*FileReader, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}
	return &FileReader{filePath, file}, nil
}

func (fr *FileReader) ReadLine() (string, error) {
	buf := make([]byte, 0, 1024)
	for {
		b := make([]byte, 1)
		_, err := fr.reader.Read(b)
		if err != nil {
			if err == io.EOF {
				break
			}
			return "", err
		}
		if b[0] == '\n' {
			break
		}
		buf = append(buf, b[0])
	}
	return string(buf), nil
}

func (fr *FileReader) ReadBytes(n int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := fr.reader.Read(buf)
	if err != nil {
		return nil, err
	}
	return buf, nil
}

func (fr *FileReader) Close() error {
	return fr.reader.Close()
}
