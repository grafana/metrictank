package main

import (
	"fmt"
	"time"

	//"github.com/dgryski/go-tsz"
	"github.com/raintank/go-tsz"
)

// Chunk is a chunk of data. not concurrency safe.
type Chunk struct {
	*tsz.Series
	T0        uint32
	LastTs    uint32 // last TS seen, not computed or anything
	NumPoints uint32
	Saved     bool
	LastWrite uint32
}

func NewChunk(t0 uint32) *Chunk {
	// we must set LastWrite here as well to make sure a new Chunk doesn't get immediately
	// garbage collected right after creating it, before we can push to it
	return &Chunk{tsz.New(t0), t0, 0, 0, false, uint32(time.Now().Unix())}
}

func (c *Chunk) String() string {
	return fmt.Sprintf("<chunk T0=%d, LastTs=%d, NumPoints=%d, Saved=%t>", c.T0, c.LastTs, c.NumPoints, c.Saved)

}
func (c *Chunk) Push(t uint32, v float64) error {
	if t <= c.LastTs {
		return fmt.Errorf("Point must be newer than already added points. t:%d lastTs: %d", t, c.LastTs)
	}
	c.Series.Push(t, v)
	c.NumPoints += 1
	c.LastTs = t
	c.LastWrite = uint32(time.Now().Unix())
	totalPoints <- 1
	return nil
}
