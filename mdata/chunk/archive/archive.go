package archive

import (
	"bytes"
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/grafana/metrictank/mdata/chunk"
	"gopkg.in/raintank/schema.v1"
)

//go:generate msgp
type Archive struct {
	RowKey          string
	SecondsPerPoint uint32
	Points          uint32
	Chunks          []chunk.IterGen
}

//go:generate msgp
type Metric struct {
	MetricData        schema.MetricData
	AggregationMethod uint32
	Archives          []Archive
}

func (m *Metric) MarshalCompressed() (*bytes.Buffer, error) {
	var buf bytes.Buffer

	b, err := m.MarshalMsg(nil)
	if err != nil {
		return &buf, errors.New(fmt.Sprintf("ERROR: Marshalling metric: %q", err))
	}

	g := gzip.NewWriter(&buf)
	_, err = g.Write(b)
	if err != nil {
		return &buf, errors.New(fmt.Sprintf("ERROR: Compressing MSGP data: %q", err))
	}

	err = g.Close()
	if err != nil {
		return &buf, errors.New(fmt.Sprintf("ERROR: Compressing MSGP data: %q", err))
	}

	return &buf, nil
}

func (m *Metric) UnmarshalCompressed(b io.Reader) error {
	gzipReader, err := gzip.NewReader(b)
	if err != nil {
		return errors.New(fmt.Sprintf("ERROR: Creating Gzip reader: %q", err))
	}

	raw, err := ioutil.ReadAll(gzipReader)
	if err != nil {
		return errors.New(fmt.Sprintf("ERROR: Decompressing Gzip: %q", err))
	}

	_, err = m.UnmarshalMsg(raw)
	if err != nil {
		return errors.New(fmt.Sprintf("ERROR: Unmarshaling Raw: %q", err))
	}

	return nil
}
