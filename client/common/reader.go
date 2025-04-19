package common

import (
	"encoding/csv"
	"io"
	"os"
)

type FileReader struct {
	filePath      string
	reader        io.ReadCloser
	csvReader     *csv.Reader
	headerSkipped bool
}

func NewFileReader(filePath string) (*FileReader, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}

	csvReader := csv.NewReader(file)

	// Crear el lector con valores iniciales
	fr := &FileReader{
		filePath:      filePath,
		reader:        file,
		csvReader:     csvReader,
		headerSkipped: false,
	}

	// Ignorar la primera línea (header) automáticamente
	if _, err := fr.skipHeader(); err != nil && err != io.EOF {
		file.Close()
		return nil, err
	}

	return fr, nil
}

// skipHeader ignora la primera línea del archivo CSV si aún no se ha ignorado
func (fr *FileReader) skipHeader() ([]string, error) {
	if !fr.headerSkipped {
		header, err := fr.csvReader.Read()
		if err != nil {
			return nil, err
		}
		fr.headerSkipped = true
		return header, nil
	}
	return nil, nil
}

// ReadCSVLine lee una línea del archivo CSV y la devuelve como un slice de strings
func (fr *FileReader) ReadCSVLine() ([]string, error) {
	record, err := fr.csvReader.Read()
	if err != nil {
		return nil, err
	}
	return record, nil
}

// ReadLine se mantiene para compatibilidad con el código existente
// pero ahora convierte la línea CSV a un string con separadores
func (fr *FileReader) ReadLine() (string, error) {
	record, err := fr.ReadCSVLine()
	if err != nil {
		if err == io.EOF {
			return "", io.EOF
		}
		return "", err
	}

	// Crear una cadena separada por comas a partir del registro CSV
	result := ""
	for i, field := range record {
		if i > 0 {
			result += ","
		}
		result += field
	}

	return result, nil
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
