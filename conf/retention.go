package conf

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	schema "gopkg.in/raintank/schema.v1"

	"github.com/grafana/metrictank/mdata/chunk"
	"github.com/raintank/dur"
)

const Month_sec = 60 * 60 * 24 * 28

type Retentions []Retention

// Validate assures the retentions are sane.  As the whisper source code says:
// An ArchiveList must:
// 1. Have at least one archive config. Example: (60, 86400)
// 2. No archive may be a duplicate of another.
// 3. Higher precision archives' precision must evenly divide all lower precision archives' precision.
// 4. Lower precision archives must cover larger time intervals than higher precision archives.
// 5. Each archive must have at least enough points to consolidate to the next archive
func (rets Retentions) Validate() error {
	if len(rets) == 0 {
		return fmt.Errorf("No retentions")
	}
	for i := 1; i < len(rets); i++ {
		prev := rets[i-1]
		ret := rets[i]

		if prev.SecondsPerPoint >= ret.SecondsPerPoint {
			return fmt.Errorf("retention must have lower resolution than prior retention")
		}

		if ret.SecondsPerPoint%prev.SecondsPerPoint != 0 {
			return fmt.Errorf("lower resolution retentions must be evenly divisible by higher resolution retentions (%d does not divide by %d)", ret.SecondsPerPoint, prev.SecondsPerPoint)
		}

		if prev.MaxRetention() >= ret.MaxRetention() {
			return fmt.Errorf("lower resolution archives must have longer retention than higher resolution archives")
		}

		if prev.NumberOfPoints < (ret.SecondsPerPoint / prev.SecondsPerPoint) {
			return fmt.Errorf("Each archive must have at least enough points to consolidate to the next archive (archive%v consolidates %v of archive%v's points but it has only %v total points)", i, ret.SecondsPerPoint/prev.SecondsPerPoint, i-1, prev.NumberOfPoints)
		}
	}
	return nil
}

/*
  A retention level.

  Retention levels describe a given archive in the database. How detailed it is and how far back
  it records.
*/
type Retention struct {
	SecondsPerPoint int    // interval in seconds
	NumberOfPoints  int    // ~ttl
	ChunkSpan       uint32 // duration of chunk of aggregated metric for storage, controls how many aggregated points go into 1 chunk
	NumChunks       uint32 // number of chunks to keep in memory. remember, for a query from now until 3 months ago, we will end up querying the memory server as well.
	Ready           bool   // ready for reads?
}

func (r Retention) MaxRetention() int {
	return r.SecondsPerPoint * r.NumberOfPoints
}

func NewRetention(secondsPerPoint, numberOfPoints int) Retention {
	return Retention{
		SecondsPerPoint: secondsPerPoint,
		NumberOfPoints:  numberOfPoints,
	}
}

func NewRetentionMT(secondsPerPoint int, ttl, chunkSpan, numChunks uint32, ready bool) Retention {
	return Retention{
		SecondsPerPoint: secondsPerPoint,
		NumberOfPoints:  int(ttl) / secondsPerPoint,
		ChunkSpan:       chunkSpan,
		NumChunks:       numChunks,
		Ready:           ready,
	}
}

// ParseRetentions parses retention definitions into a Retentions structure
func ParseRetentions(defs string) (Retentions, error) {
	retentions := make(Retentions, 0)
	for i, def := range strings.Split(defs, ",") {
		def = strings.TrimSpace(def)
		parts := strings.Split(def, ":")
		if len(parts) < 2 || len(parts) > 5 {
			return nil, fmt.Errorf("bad retentions spec %q", def)
		}

		// try old format
		val1, err1 := strconv.ParseInt(parts[0], 10, 0)
		val2, err2 := strconv.ParseInt(parts[1], 10, 0)

		var retention Retention
		var err error
		if err1 == nil && err2 == nil {
			retention = NewRetention(int(val1), int(val2))
		} else {
			// try new format
			retention, err = ParseRetentionNew(def)
			if err != nil {
				return nil, err
			}
		}
		if i != 0 && !schema.IsSpanValid(uint32(retention.SecondsPerPoint)) {
			return nil, fmt.Errorf("invalid retention: can't encode span of %d", retention.SecondsPerPoint)

		}
		if len(parts) >= 3 {
			retention.ChunkSpan, err = dur.ParseNDuration(parts[2])
			if err != nil {
				return nil, err
			}
			if (Month_sec % retention.ChunkSpan) != 0 {
				return nil, errors.New("chunkSpan must fit without remainders into month_sec (28*24*60*60)")
			}
			_, ok := chunk.RevChunkSpans[retention.ChunkSpan]
			if !ok {
				return nil, fmt.Errorf("chunkSpan %s is not a valid value (https://github.com/grafana/metrictank/blob/master/docs/memory-server.md#valid-chunk-spans)", parts[2])
			}
		} else {
			// default to a valid chunkspan that can hold at least 100 points, or select the largest one otherwise.
			approxSpan := uint32(retention.SecondsPerPoint * 100)
			var span uint32
			for _, span = range chunk.ChunkSpans {
				if span >= approxSpan {
					break
				}
			}
			retention.ChunkSpan = span
		}
		retention.NumChunks = 2
		if len(parts) >= 4 {
			i, err := strconv.Atoi(parts[3])
			if err != nil {
				return nil, err
			}
			retention.NumChunks = uint32(i)
		}
		retention.Ready = true
		if len(parts) == 5 {
			retention.Ready, err = strconv.ParseBool(parts[4])
			if err != nil {
				return nil, err
			}
		}

		retentions = append(retentions, retention)
	}
	return retentions, retentions.Validate()
}

func ParseRetentionNew(def string) (Retention, error) {
	parts := strings.Split(def, ":")
	if len(parts) < 2 {
		return Retention{}, fmt.Errorf("Not enough parts in retentionDef %q", def)
	}
	interval, err := dur.ParseDuration(parts[0])
	if err != nil {
		return Retention{}, fmt.Errorf("Failed to parse interval in %q: %s", def, err)
	}

	ttl, err := dur.ParseDuration(parts[1])
	if err != nil {
		return Retention{}, fmt.Errorf("Failed to parse TTL in %q: %s", def, err)
	}

	return Retention{
		SecondsPerPoint: int(interval),
		NumberOfPoints:  int(ttl / interval)}, err
}
