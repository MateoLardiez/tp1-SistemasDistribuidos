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
	result += "\n"

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

func (fr *FileReader) CreateBatch(cantLines int) ([]byte, error) {
	var batch []byte
	lineCount := 0
	const MAX_BATCH_SIZE = 8 * 1024 // 8KB

	// Leer líneas hasta alcanzar el límite de líneas o tamaño
	for lineCount < cantLines && len(batch) < MAX_BATCH_SIZE {
		record, err := fr.ReadCSVLine()

		// Manejar explícitamente el EOF
		if err == io.EOF {
			// Si ya hemos leído algo, devolvemos lo que tenemos
			if len(batch) > 0 {
				// Añadir el carácter de nueva línea al final del batch si cabe
				if len(batch)+1 <= MAX_BATCH_SIZE {
					batch = append(batch, '\n')
				}
				return batch, nil
			}
			// Si no hemos leído nada y es EOF, indicamos que ya no hay más datos
			return nil, io.EOF
		} else if err != nil {
			return nil, err
		}

		// Crear una cadena separada por comas a partir del registro CSV
		lineStr := ""
		for i, field := range record {
			if i > 0 {
				lineStr += ","
			}
			lineStr += field
		}
		lineStr += "\n"

		// Convertir la línea a bytes
		lineBytes := []byte(lineStr)

		// Para todas las líneas excepto la primera, añadimos el separador '|'
		if lineCount > 0 && len(batch) > 0 {
			// Verificar si añadir el separador superaría el tamaño máximo
			if len(batch)+1+len(lineBytes) > MAX_BATCH_SIZE {
				break
			}
			batch = append(batch, '|')
		}

		// Verificar si añadir esta línea superaría el tamaño máximo
		if len(batch)+len(lineBytes) > MAX_BATCH_SIZE {
			break
		}

		// Añadir la línea al batch
		batch = append(batch, lineBytes...)
		lineCount++
	}

	// Añadir el carácter de nueva línea al final del batch si cabe
	if len(batch)+1 <= MAX_BATCH_SIZE {
		batch = append(batch, '\n')
	}

	return batch, nil
}
