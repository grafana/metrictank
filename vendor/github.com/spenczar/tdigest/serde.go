package tdigest

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"math"
)

const (
	magic           = int16(0xc80)
	encodingVersion = int32(1)
)

func marshalBinary(d *TDigest) ([]byte, error) {
	buf := bytes.NewBuffer(nil)
	w := &binaryBufferWriter{buf: buf}
	w.writeValue(magic)
	w.writeValue(encodingVersion)
	w.writeValue(d.compression)
	w.writeValue(int32(len(d.centroids)))
	for _, c := range d.centroids {
		w.writeValue(c.count)
		w.writeValue(c.mean)
	}

	if w.err != nil {
		return nil, w.err
	}
	return buf.Bytes(), nil
}

func unmarshalBinary(d *TDigest, p []byte) error {
	var (
		mv int16
		ev int32
		n  int32
	)
	r := &binaryReader{r: bytes.NewReader(p)}
	r.readValue(&mv)
	if r.err != nil {
		return r.err
	}
	if mv != magic {
		return fmt.Errorf("data corruption detected: invalid header magic value 0x%04x", mv)
	}
	r.readValue(&ev)
	if r.err != nil {
		return r.err
	}
	if ev != encodingVersion {
		return fmt.Errorf("data corruption detected: invalid encoding version %d", ev)
	}
	r.readValue(&d.compression)
	r.readValue(&n)
	if r.err != nil {
		return r.err
	}
	if n < 0 {
		return fmt.Errorf("data corruption detected: number of centroids cannot be negative, have %v", n)

	}
	if n > 1<<20 {
		return fmt.Errorf("invalid n, cannot be greater than 2^20: %v", n)
	}
	d.centroids = make([]*centroid, int(n))
	for i := 0; i < int(n); i++ {
		c := new(centroid)
		r.readValue(&c.count)
		r.readValue(&c.mean)
		if r.err != nil {
			return r.err
		}
		if c.count < 0 {
			return fmt.Errorf("data corruption detected: negative count: %d", c.count)
		}
		if math.IsNaN(c.mean) {
			return fmt.Errorf("data corruption detected: NaN mean not permitted")
		}
		if math.IsInf(c.mean, 0) {
			return fmt.Errorf("data corruption detected: Inf mean not permitted")
		}
		if i > 0 {
			prev := d.centroids[i-1]
			if c.mean < prev.mean {
				return fmt.Errorf("data corruption detected: centroid %d has lower mean (%v) than preceding centroid %d (%v)", i, c.mean, i-1, prev.mean)
			}
		}
		d.centroids[i] = c
		if c.count > math.MaxInt64-d.countTotal {
			return fmt.Errorf("data corruption detected: centroid total size overflow")
		}
		d.countTotal += c.count
	}

	if n := r.r.Len(); n > 0 {
		return fmt.Errorf("found %d unexpected bytes trailing the tdigest", n)
	}

	return nil
}

type binaryBufferWriter struct {
	buf *bytes.Buffer
	err error
}

func (w *binaryBufferWriter) writeValue(v interface{}) {
	if w.err != nil {
		return
	}
	w.err = binary.Write(w.buf, binary.LittleEndian, v)
}

type binaryReader struct {
	r   *bytes.Reader
	err error
}

func (r *binaryReader) readValue(v interface{}) {
	if r.err != nil {
		return
	}
	r.err = binary.Read(r.r, binary.LittleEndian, v)
	if r.err == io.EOF {
		r.err = io.ErrUnexpectedEOF
	}
}
