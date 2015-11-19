package main

import (
	"bytes"
	"encoding/gob"
	"fmt"

	//"github.com/dgryski/go-tsz"
	"github.com/raintank/go-tsz"
)

// Chunk is a chunk of data. not concurrency safe.
type Chunk struct {
	*tsz.Series
	T0        uint32
	LastTs    uint32
	NumPoints uint32
	Saved     bool
}

func NewChunk(t0 uint32) *Chunk {
	return &Chunk{tsz.New(t0), t0, 0, 0, false}
}

func (c *Chunk) String() string {
	return fmt.Sprintf("<chunk t0 at %s, %d points>", TS(c.T0), c.NumPoints)

}
func (c *Chunk) Push(t uint32, v float64) error {
	if t <= c.LastTs {
		return fmt.Errorf("Can't push points that are older then points already added. t:%d lastTs: %d", t, c.LastTs)
	}
	c.Series.Push(t, v)
	c.NumPoints += 1
	c.LastTs = t
	return nil
}

type chunkOnDisk struct {
	Series    *tsz.Series
	T0        uint32
	LastTs    uint32
	NumPoints uint32
	Saved     bool
}

func (c *Chunk) GobEncode() ([]byte, error) {
	// create an OnDisk format of our data.
	cOnDisk := chunkOnDisk{
		Series:    c.Series,
		T0:        c.T0,
		LastTs:    c.LastTs,
		NumPoints: c.NumPoints,
		Saved:     c.Saved,
	}
	var b bytes.Buffer
	enc := gob.NewEncoder(&b)
	err := enc.Encode(cOnDisk)
	return b.Bytes(), err
}

func (c *Chunk) GobDecode(data []byte) error {
	//decode our data bytes into our onDisk struct
	r := bytes.NewReader(data)
	dec := gob.NewDecoder(r)
	cOnDisk := &chunkOnDisk{}
	err := dec.Decode(cOnDisk)
	if err != nil {
		return err
	}

	// fill in the fields of the passed Chunk with the data from our OnDisk format.
	c.Series = cOnDisk.Series
	c.T0 = cOnDisk.T0
	c.LastTs = cOnDisk.LastTs
	c.NumPoints = cOnDisk.NumPoints
	c.Saved = cOnDisk.Saved

	return nil
}
