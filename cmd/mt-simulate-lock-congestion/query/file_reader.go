package query

import (
	"github.com/grafana/metrictank/cmd/mt-simulate-lock-congestion/reader"
)

type QueriesFromFile struct {
	reader *reader.FileReader
}

func NewQueriesFromFile(reader *reader.FileReader) (QueryGenerator, error) {
	return &QueriesFromFile{reader: reader}, nil
}

func (f *QueriesFromFile) Start() {
	f.reader.Start()
}

func (f *QueriesFromFile) GetPattern() string {
	return f.reader.GetRandomLine()
}
