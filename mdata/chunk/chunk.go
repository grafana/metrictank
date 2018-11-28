// package chunk encodes timeseries in chunks of data
// see devdocs/chunk-format.md for more information.
package chunk

import (
	"fmt"

	"github.com/grafana/metrictank/mdata/chunk/tsz"
)

// Chunk is a chunk of data. not concurrency safe.
// last check that the methods are being called safely by Dieter on 20/11/2018
// checked: String, Push, Finish, Encode and properties Series, NumPoints, First
// for the most part, confirming serialized access is easy by tracking all callers/references.
// The main exception is the ChunkWriteRequest mechanism. any CWR created is processed
// asynchronously, and chunk properties will be read (the series.T0, chunk.Encode(), etc)
// But it can be proven that we only call NewChunkWriteRequest() on chunks that are no longer being modified.
type Chunk struct {
	Series    tsz.SeriesLong
	NumPoints uint32
	First     bool
}

func New(t0 uint32) *Chunk {
	return &Chunk{
		Series: *tsz.NewSeriesLong(t0),
	}
}

func NewFirst(t0 uint32) *Chunk {
	return &Chunk{
		Series: *tsz.NewSeriesLong(t0),
		First:  true,
	}
}

func (c *Chunk) String() string {
	return fmt.Sprintf("<chunk T0=%d, LastTs=%d, NumPoints=%d, First=%t, Closed=%t>", c.Series.T0, c.Series.T, c.NumPoints, c.First, c.Series.Finished)
}

func (c *Chunk) Push(t uint32, v float64) error {
	if t <= c.Series.T {
		return fmt.Errorf("Point must be newer than already added points. t:%d lastTs: %d", t, c.Series.T)
	}
	c.Series.Push(t, v)
	c.NumPoints++
	return nil
}

func (c *Chunk) Finish() {
	c.Series.Finish()
}

// Encode encodes the chunk
// note: chunks don't know their own span, the caller/owner manages that,
// so for formats that encode it, it needs to be passed in.
// the returned value contains no references to the chunk. data is copied.
func (c *Chunk) Encode(span uint32) []byte {
	return encode(span, FormatGoTszLongWithSpan, c.Series.Bytes())
}
