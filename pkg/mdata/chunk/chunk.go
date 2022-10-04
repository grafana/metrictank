// package chunk encodes timeseries in chunks of data
// see devdocs/chunk-format.md for more information.
package chunk

import (
	"fmt"

	"github.com/grafana/metrictank/pkg/mdata/chunk/tsz"
	"github.com/grafana/metrictank/pkg/mdata/errors"
)

// Chunk is a chunk of data. not concurrency safe.
// last check that all properties and methods are being accessed safely by Dieter on 2021-10-06
// for the most part, confirming serialized access is easy by tracking all callers/references.
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
	if t == c.Series.T {
		return errors.ErrMetricNewValueForTimestamp
	} else if t < c.Series.T {
		return errors.ErrMetricTooOld
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
