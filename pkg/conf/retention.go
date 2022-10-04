package conf

import (
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/grafana/metrictank/pkg/schema"

	"github.com/grafana/metrictank/pkg/mdata/chunk"
	"github.com/raintank/dur"
)

const Month_sec = 60 * 60 * 24 * 28

var errReadyFormat = errors.New("'ready' field must be a bool or unsigned integer")

type Retentions struct {
	Orig string
	Rets []Retention
}

// Sub returns a "subslice" of Retentions starting at the given pos.
func (r Retentions) Sub(pos int) Retentions {
	origSplit := strings.Split(r.Orig, ",")
	return Retentions{
		Orig: strings.Join(origSplit[pos:], ","),
		Rets: r.Rets[pos:],
	}
}

func BuildFromRetentions(rets ...Retention) Retentions {
	return Retentions{
		Orig: buildOrigFromRetentions(rets),
		Rets: rets,
	}
}
func buildOrigFromRetentions(rets []Retention) string {
	var out []string
	for _, r := range rets {
		out = append(out, r.String())
	}
	return strings.Join(out, ",")
}

// Validate assures the retentions are sane.  As the whisper source code says:
// An ArchiveList must:
// 1. Have at least one archive config. Example: (60, 86400)
// 2. No archive may be a duplicate of another.
// 3. Higher precision archives' precision must evenly divide all lower precision archives' precision.
// 4. Lower precision archives must cover larger time intervals than higher precision archives.
// 5. Each archive must have at least enough points to consolidate to the next archive
func (r Retentions) Validate() error {
	if len(r.Rets) == 0 {
		return fmt.Errorf("No retentions")
	}
	for i := 1; i < len(r.Rets); i++ {
		prev := r.Rets[i-1]
		ret := r.Rets[i]

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
	Ready           uint32 // ready for reads for data as of this timestamp (or as of now-TTL, whichever is highest)
}

func (r Retention) MaxRetention() int {
	return r.SecondsPerPoint * r.NumberOfPoints
}

func (r Retention) String() string {
	s := dur.FormatDuration(uint32(r.SecondsPerPoint))
	s += ":" + dur.FormatDuration(uint32(r.NumberOfPoints*r.SecondsPerPoint))
	s += ":" + dur.FormatDuration(r.ChunkSpan)
	s += ":" + strconv.Itoa(int(r.NumChunks))
	switch r.Ready {
	case 0:
		s += ":true"
	case math.MaxUint32:
		s += ":false"
	default:
		s += ":" + strconv.FormatUint(uint64(r.Ready), 10)
	}
	return s
}

func NewRetention(secondsPerPoint, numberOfPoints int) Retention {
	return Retention{
		SecondsPerPoint: secondsPerPoint,
		NumberOfPoints:  numberOfPoints,
	}
}

func NewRetentionMT(secondsPerPoint int, ttl, chunkSpan, numChunks, ready uint32) Retention {
	return Retention{
		SecondsPerPoint: secondsPerPoint,
		NumberOfPoints:  int(ttl) / secondsPerPoint,
		ChunkSpan:       chunkSpan,
		NumChunks:       numChunks,
		Ready:           ready,
	}
}

func MustParseRetentions(defs string) Retentions {
	r, err := ParseRetentions(defs)
	if err != nil {
		panic(err)
	}
	return r
}

// ParseRetentions parses retention definitions into a Retentions structure
func ParseRetentions(defs string) (Retentions, error) {
	retentions := Retentions{
		Orig: defs,
	}
	cnt := strings.Count(defs, ",")
	if cnt > 254 {
		return retentions, errors.New("no more than 255 individual retensions per rule supported")
	}
	for i, def := range strings.Split(defs, ",") {
		def = strings.TrimSpace(def)
		parts := strings.Split(def, ":")
		if len(parts) < 2 || len(parts) > 5 {
			return retentions, fmt.Errorf("bad retentions spec %q", def)
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
				return retentions, err
			}
		}
		if i != 0 && !schema.IsSpanValid(uint32(retention.SecondsPerPoint)) {
			return retentions, fmt.Errorf("invalid retention: can't encode span of %d", retention.SecondsPerPoint)

		}
		if len(parts) >= 3 {
			retention.ChunkSpan, err = dur.ParseNDuration(parts[2])
			if err != nil {
				return retentions, err
			}
			if (Month_sec % retention.ChunkSpan) != 0 {
				return retentions, errors.New("chunkSpan must fit without remainders into month_sec (28*24*60*60)")
			}
			_, ok := chunk.RevChunkSpans[retention.ChunkSpan]
			if !ok {
				return retentions, fmt.Errorf("chunkSpan %s is not a valid value (https://github.com/grafana/metrictank/blob/master/docs/memory-server.md#valid-chunk-spans)", parts[2])
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
				return retentions, err
			}
			retention.NumChunks = uint32(i)
		}
		if len(parts) == 5 {
			// user is allowed to specify both a bool or a timestamp.
			// internally we map both to timestamp.
			// 0 (default) is effectively the same as 'true'
			// math.MaxUint32 is effectively the same as 'false'
			readyInt, err := strconv.ParseUint(parts[4], 10, 32)
			if err == nil {
				retention.Ready = uint32(readyInt)
			} else {
				readyBool, err := strconv.ParseBool(parts[4])
				if err != nil {
					return retentions, errReadyFormat
				}
				if !readyBool {
					retention.Ready = math.MaxUint32
				}
			}
		}

		retentions.Rets = append(retentions.Rets, retention)
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
