package metricname

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sync/atomic"
)

type fileGenerator struct {
	file   *os.File
	reader *bufio.Reader
	names  []string
	cursor uint32
}

func NewFileGenerator(filename string) (NameGenerator, error) {
	res := fileGenerator{}
	var err error
	res.file, err = os.Open(filename)
	if err != nil {
		return nil, fmt.Errorf("Failed to open file \"%s\": %s", filename, err)
	}

	res.reader = bufio.NewReaderSize(res.file, 1024)

	return &res, nil
}

func (f *fileGenerator) GetNewMetricName() string {
	pos := atomic.AddUint32(&f.cursor, 1)
	if pos >= uint32(len(f.names)) {
		log.Fatal("Exhausted the whole file")
	}

	return f.names[pos]
}

func (f *fileGenerator) GetExistingMetricName() string {
	if len(f.names) == 0 {
		return ""
	}

	pos := atomic.LoadUint32(&f.cursor)

	return f.names[pos/2]
}

func (f *fileGenerator) Start(_ context.Context, _ uint32) {
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
			f.names = append(f.names, line)
			line = line[:0]
		}
	}
}
