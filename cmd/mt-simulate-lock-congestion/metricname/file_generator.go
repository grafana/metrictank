package metricname

import (
	"context"

	"github.com/grafana/metrictank/cmd/mt-simulate-lock-congestion/reader"
)

type FileGenerator struct {
	reader *reader.FileReader
}

func NewFileGenerator(reader *reader.FileReader) (NameGenerator, error) {
	return &FileGenerator{
		reader: reader,
	}, nil
}

func (f *FileGenerator) GetNewMetricName() string {
	return f.reader.GetNextLine()
}

func (f *FileGenerator) Start(_ context.Context, _ uint32) {
	f.reader.Start()
}
