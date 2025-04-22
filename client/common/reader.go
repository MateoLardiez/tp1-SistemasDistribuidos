package common

import (
	"bytes"
	"encoding/csv"
	"io"
	"os"
	"strings"
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
	// Configurar el CSVReader para manejar correctamente campos con saltos de línea
	csvReader.LazyQuotes = true
	csvReader.FieldsPerRecord = -1 // Permite cualquier número de campos

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

// ToCSVLine convierte un slice de strings a una línea CSV bien formateada
// respetando el RFC 4180 (especialmente para campos con saltos de línea y comillas)
func ToCSVLine(record []string) string {
	// Utilizamos el paquete csv de Go para formatear correctamente la línea
	buf := &bytes.Buffer{}
	writer := csv.NewWriter(buf)

	// Escribimos el record usando el csv.Writer que maneja correctamente
	// los casos especiales como campos con comillas o saltos de línea
	writer.Write(record)
	writer.Flush()

	// Eliminamos el salto de línea final que añade el writer
	return strings.TrimRight(buf.String(), "\r\n")
}

// ReadLine se mantiene para compatibilidad con el código existente
// pero ahora usa ToCSVLine para formatear correctamente la línea CSV
func (fr *FileReader) ReadLine() (string, error) {
	record, err := fr.ReadCSVLine()
	if err != nil {
		if err == io.EOF {
			return "", io.EOF
		}
		return "", err
	}

	// Usar la función ToCSVLine para formatear correctamente respetando RFC 4180
	return ToCSVLine(record), nil
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
