package mdata

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"time"

	"github.com/tinylib/msgp/msgp"

	"github.com/raintank/schema"
)

type ChunkSaveCallback func()

// ChunkWriteRequest is a request to write a chunk into a store
//go:generate msgp
type ChunkWriteRequest struct {
	Callback  ChunkSaveCallback `msg:"-"`
	Key       schema.AMKey      `msg:"key,extension"`
	TTL       uint32
	T0        uint32
	Data      []byte
	Timestamp time.Time
}

// NewChunkWriteRequest creates a new ChunkWriteRequest
func NewChunkWriteRequest(callback ChunkSaveCallback, key schema.AMKey, ttl, t0 uint32, data []byte, ts time.Time) ChunkWriteRequest {
	return ChunkWriteRequest{callback, key, ttl, t0, data, ts}
}

// ArchiveRequest is a complete representation of a Metric together with some
// chunk write requests containing data that shall be written into this metric
//go:generate msgp
type ArchiveRequest struct {
	MetricData         schema.MetricData
	ChunkWriteRequests []ChunkWriteRequest
}

func (a *ArchiveRequest) MarshalCompressed() (*bytes.Buffer, error) {
	var buf bytes.Buffer

	buf.WriteByte(byte(uint8(1)))

	g := gzip.NewWriter(&buf)
	err := msgp.Encode(g, a)
	if err != nil {
		return &buf, fmt.Errorf("ERROR: Encoding MGSP data: %q", err)
	}

	err = g.Close()
	if err != nil {
		return &buf, fmt.Errorf("ERROR: Compressing MSGP data: %q", err)
	}

	return &buf, nil
}

func (a *ArchiveRequest) UnmarshalCompressed(b io.Reader) error {
	versionBuf := make([]byte, 1)
	readBytes, err := b.Read(versionBuf)
	if err != nil || readBytes != 1 {
		return fmt.Errorf("ERROR: Failed to read one byte: %s", err)
	}

	version := uint8(versionBuf[0])
	if version != 1 {
		return fmt.Errorf("ERROR: Only version 1 is supported, received version %d", version)
	}

	gzipReader, err := gzip.NewReader(b)
	if err != nil {
		return fmt.Errorf("ERROR: Creating Gzip reader: %q", err)
	}

	err = msgp.Decode(bufio.NewReader(gzipReader), a)
	if err != nil {
		return fmt.Errorf("ERROR: Unmarshaling Raw: %q", err)
	}
	gzipReader.Close()

	return nil
}
