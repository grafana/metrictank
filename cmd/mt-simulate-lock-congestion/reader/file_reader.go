package reader

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"sync/atomic"
)

type FileReader struct {
	file   *os.File
	reader *bufio.Reader
	lines  []string
	cursor uint32
}

func NewFileReader(filename string) (*FileReader, error) {
	res := FileReader{}
	var err error
	res.file, err = os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("Failed to open file \"%s\": %s", filename, err)
	}

	res.reader = bufio.NewReaderSize(res.file, 1024)

	return &res, nil
}

func (f *FileReader) GetNextLine() string {
	pos := atomic.AddUint32(&f.cursor, 1)
	if pos >= uint32(len(f.lines)) {
		log.Fatal("Exhausted the whole file")
	}

	return f.lines[pos]
}

func (f *FileReader) GetRandomLine() string {
	return f.lines[rand.Intn(len(f.lines))]
}

func (f *FileReader) Start() {
	var isPrefix bool
	var line string
	var buf []byte
	var err error

LINES:
	for {
		isPrefix = true
		for isPrefix == true {
			buf, isPrefix, err = f.reader.ReadLine()
			if err != nil {
				if err == io.EOF {
					break LINES
				}
				log.Fatalf("Error when reading from file: %s", err)
			}
			line = line + string(buf)
		}

		if len(line) > 0 {
			f.lines = append(f.lines, line)
			line = line[:0]
		}
	}
}
