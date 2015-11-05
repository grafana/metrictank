package main

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/dgryski/go-tsz"
	"log"
)

// Chunk is a chunk of data. not concurrency safe.
type Chunk struct {
	*tsz.Series
	T0        uint32
	NumPoints uint32
	Saved     bool
}

func NewChunk(t0 uint32) *Chunk {
	return &Chunk{tsz.New(t0), t0, 0, false}
}

func (c *Chunk) String() string {
	return fmt.Sprintf("<chunk t0 at %s, %d points>", TS(c.T0), c.NumPoints)

}
func (c *Chunk) Push(t uint32, v float64) *Chunk {
	c.Series.Push(t, v)
	c.NumPoints += 1
	return c
}

type chunkOnDisk struct {
	T0        uint32
	NumPoints uint32
	Saved     bool
	Series    tsz.Series
}

func (c *Chunk) GobEncode() ([]byte, error) {
	log.Println("marshaling chunk to Binary")
	cOnDisk := chunkOnDisk{
		T0:        c.T0,
		NumPoints: c.NumPoints,
		Saved:     c.Saved,
		Series:    *c.Series,
	}
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	err := enc.Encode(cOnDisk)

	return b.Bytes(), err
}

func (c *Chunk) GobDecode(data []byte) error {
	log.Println("chunk unmarshaling to Binary")
	r := bytes.NewReader(data)
	dec := gob.NewDecoder(r)
	cOnDisk := &chunkOnDisk{}
	err := dec.Decode(cOnDisk)
	if err != nil {
		return err
	}
	c.Series = &cOnDisk.Series
	c.T0 = cOnDisk.T0
	c.NumPoints = cOnDisk.NumPoints
	c.Saved = cOnDisk.Saved
	return nil
}
