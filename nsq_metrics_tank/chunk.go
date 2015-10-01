package main

import (
	"fmt"

	"github.com/dgryski/go-tsz"
)

// Chunk is a chunk of data. not concurrency safe.
type Chunk struct {
	*tsz.Series
	t0        uint32
	numPoints uint32
	saved     bool
}

func NewChunk(t0 uint32) *Chunk {
	return &Chunk{tsz.New(t0), t0, 0, false}
}

func (c *Chunk) String() string {
	return fmt.Sprintf("<chunk t0 at %s, %d points>", ts(c.t0), c.numPoints)

}
func (c *Chunk) Push(t uint32, v float64) *Chunk {
	c.Series.Push(t, v)
	c.numPoints += 1
	return c
}
