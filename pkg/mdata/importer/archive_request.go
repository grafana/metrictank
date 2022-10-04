package importer

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"time"

	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/schema"
	"github.com/kisielk/whisper-go/whisper"
	"github.com/tinylib/msgp/msgp"
)

//go:generate msgp

// ArchiveRequest is a complete representation of a Metric together with some
// chunk write requests containing data that shall be written into this metric
type ArchiveRequest struct {
	MetricData         schema.MetricData
	ChunkWriteRequests []ChunkWriteRequest
}

func NewArchiveRequest(w *whisper.Whisper, schemas conf.Schemas, file, name string, from, until uint32, writeUnfinishedChunks bool) (*ArchiveRequest, error) {
	if len(w.Header.Archives) == 0 {
		return nil, fmt.Errorf("Whisper file contains no archives: %q", file)
	}

	method, err := convertWhisperMethod(w.Header.Metadata.AggregationMethod)
	if err != nil {
		return nil, err
	}

	points := make(map[int][]whisper.Point)
	for i := range w.Header.Archives {
		p, err := w.DumpArchive(i)
		if err != nil {
			return nil, fmt.Errorf("Failed to dump archive %d from whisper file %s", i, file)
		}
		points[i] = p
	}

	res := &ArchiveRequest{
		MetricData: schema.MetricData{
			Name:     name,
			Value:    0,
			Interval: int(w.Header.Archives[0].SecondsPerPoint),
			Unit:     "unknown",
			Time:     0,
			Mtype:    "gauge",
			Tags:     []string{},
		},
	}
	res.MetricData.SetId()

	_, selectedSchema := schemas.Match(res.MetricData.Name, int(w.Header.Archives[0].SecondsPerPoint))
	converter := newConverter(w.Header.Archives, points, method, from, until)
	for retIdx, retention := range selectedSchema.Retentions.Rets {
		convertedPoints := converter.getPoints(retIdx, uint32(retention.SecondsPerPoint), uint32(retention.NumberOfPoints))
		for m, p := range convertedPoints {
			if len(p) == 0 {
				continue
			}

			var archive schema.Archive
			if retIdx > 0 {
				archive = schema.NewArchive(m, uint32(retention.SecondsPerPoint))
			}

			encodedChunks := encodeChunksFromPoints(p, uint32(retention.SecondsPerPoint), retention.ChunkSpan, writeUnfinishedChunks)
			for _, chunk := range encodedChunks {
				res.ChunkWriteRequests = append(res.ChunkWriteRequests, NewChunkWriteRequest(
					archive,
					uint32(retention.MaxRetention()),
					chunk.Series.T0,
					chunk.Encode(retention.ChunkSpan),
					time.Now(),
				))
			}

			if res.MetricData.Time < int64(p[len(p)-1].Timestamp) {
				res.MetricData.Time = int64(p[len(p)-1].Timestamp)
			}
		}
	}

	return res, nil
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
