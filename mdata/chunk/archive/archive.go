package archive

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"

	"github.com/kisielk/whisper-go/whisper"
	"gopkg.in/raintank/schema.v1"
)

type MetricChunk struct {
	ChunkSpan uint32
	Bytes     []byte
}

// indexed by T0
type ArchiveOfChunks map[uint32]MetricChunk

type Archive struct {
	MetricData  schema.MetricData
	ArchiveInfo whisper.ArchiveInfo
	Chunks      ArchiveOfChunks
}

type Metric struct {
	Metadata whisper.Metadata
	Archives []Archive
}

func NewMetricFromEncoded(body io.Reader) *Metric {
	m := &Metric{}
	m.Decode(body)
	return m
}

func (m *Metric) Encode() (*bytes.Buffer, error) {
	b, err := json.Marshal(m)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("ERROR: Failed to marshal metric data: %s", err))
	}

	var buf bytes.Buffer
	g := gzip.NewWriter(&buf)
	_, err = g.Write(b)
	if err != nil {
		return nil, errors.New(fmt.Sprintf("ERROR: Compressing JSON data: %s", err))
	}
	err = g.Close()
	if err != nil {
		return nil, errors.New(fmt.Sprintf("ERROR: Compressing JSON data: %s", err))
	}

	return &buf, nil
}

func (m *Metric) Decode(body io.Reader) error {
	gzipReader, err := gzip.NewReader(body)
	if err != nil {
		return errors.New(fmt.Sprintf("Error creating gzip reader: %s", err))
	}

	raw, err := ioutil.ReadAll(gzipReader)
	if err != nil {
		return errors.New(fmt.Sprintf("Error reading from gzip reader: %s", err))
	}

	err = json.Unmarshal(raw, m)
	if err != nil {
		return errors.New(fmt.Sprintf("Error unmarshaling json: %s", err))
	}

	return nil
}
