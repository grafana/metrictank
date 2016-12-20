package chunk

import (
	"fmt"
	"time"

	"github.com/dgryski/go-tsz"
	"github.com/raintank/metrictank/stats"
)

var totalPoints = stats.NewGauge64("tank.total_points")

// Chunk is a chunk of data. not concurrency safe.
type Chunk struct {
	tsz.Series
	LastTs    uint32 // last TS seen, not computed or anything
	NumPoints uint32
	Saved     bool
	Saving    bool
	LastWrite uint32
}

func New(t0 uint32) *Chunk {
	// we must set LastWrite here as well to make sure a new Chunk doesn't get immediately
	// garbage collected right after creating it, before we can push to it
	return &Chunk{*tsz.New(t0), 0, 0, false, false, uint32(time.Now().Unix())}
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
	totalPoints.Inc()
	return nil
}

func (c *Chunk) Clear() {
	totalPoints.DecUint64(uint64(c.NumPoints))
}
