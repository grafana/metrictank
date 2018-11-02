package chunk

import (
	"fmt"

	"github.com/grafana/metrictank/mdata/chunk/tsz"
)

// Chunk is a chunk of data. not concurrency safe.
type Chunk struct {
	tsz.Series4h
	LastTs    uint32 // last TS seen, not computed or anything
	NumPoints uint32
	First     bool
	Closed    bool
}

func New(t0 uint32) *Chunk {
	return &Chunk{
		Series4h: *tsz.NewSeries4h(t0),
	}
}

func NewFirst(t0 uint32) *Chunk {
	return &Chunk{
		Series4h: *tsz.NewSeries4h(t0),
		First:    true,
	}
}

func (c *Chunk) String() string {
	return fmt.Sprintf("<chunk T0=%d, LastTs=%d, NumPoints=%d, First=%t, Closed=%t>", c.T0, c.LastTs, c.NumPoints, c.First, c.Closed)

}
func (c *Chunk) Push(t uint32, v float64) error {
	if t <= c.LastTs {
		return fmt.Errorf("Point must be newer than already added points. t:%d lastTs: %d", t, c.LastTs)
	}
	c.Series4h.Push(t, v)
	c.NumPoints += 1
	c.LastTs = t
	return nil
}

func (c *Chunk) Finish() {
	c.Closed = true
	c.Series4h.Finish()
}

// Encode encodes the chunk in the requested format.
// note: chunks don't know their own span, the caller/owner manages that,
// so for formats that encode it, it needs to be passed in.
func (c *Chunk) Encode(span uint32, format Format) []byte {
	return encode(span, format, c.Series4h.Bytes())
}
