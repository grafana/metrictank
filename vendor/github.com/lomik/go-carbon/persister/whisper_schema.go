// this is a parser for graphite's storage-schemas.conf
// it supports old and new retention format
// see https://graphite.readthedocs.io/en/0.9.9/config-carbon.html#storage-schemas-conf
// based on https://github.com/grobian/carbonwriter but with some improvements
package persister

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/alyu/configparser"
	"github.com/lomik/go-whisper"
	"github.com/raintank/dur"
	"github.com/raintank/metrictank/mdata/chunk"
)

const Month_sec = 60 * 60 * 24 * 28

var ChunkSpans = [32]uint32{
	1,
	5,
	10,
	15,
	20,
	30,
	60,        // 1m
	90,        // 1.5m
	2 * 60,    // 2m
	3 * 60,    // 3m
	5 * 60,    // 5m
	10 * 60,   // 10m
	15 * 60,   // 15m
	20 * 60,   // 20m
	30 * 60,   // 30m
	45 * 60,   // 45m
	3600,      // 1h
	90 * 60,   // 1.5h
	2 * 3600,  // 2h
	150 * 60,  // 2.5h
	3 * 3600,  // 3h
	4 * 3600,  // 4h
	5 * 3600,  // 5h
	6 * 3600,  // 6h
	7 * 3600,  // 7h
	8 * 3600,  // 8h
	9 * 3600,  // 9h
	10 * 3600, // 10h
	12 * 3600, // 12h
	15 * 3600, // 15h
	18 * 3600, // 18h
	24 * 3600, // 24h
}

type SpanCode uint8

var RevChunkSpans = make(map[uint32]SpanCode, len(ChunkSpans))

func init() {
	for k, v := range ChunkSpans {
		RevChunkSpans[v] = SpanCode(k)
	}
}

// Schema represents one schema setting
type Schema struct {
	Name         string
	Pattern      *regexp.Regexp
	RetentionStr string
	Retentions   whisper.Retentions
	Priority     int64
}

// WhisperSchemas contains schema settings
type WhisperSchemas []Schema

func (s WhisperSchemas) Len() int           { return len(s) }
func (s WhisperSchemas) Swap(i, j int)      { s[i], s[j] = s[j], s[i] }
func (s WhisperSchemas) Less(i, j int) bool { return s[i].Priority >= s[j].Priority }

// Match finds the schema for metric or returns false if none found
func (s WhisperSchemas) Match(metric string) (Schema, bool) {
	for _, schema := range s {
		if schema.Pattern.MatchString(metric) {
			return schema, true
		}
	}
	return Schema{}, false
}

// ParseRetentionDefs parses retention definitions into a Retentions structure
func ParseRetentionDefs(retentionDefs string) (whisper.Retentions, error) {
	retentions := make(whisper.Retentions, 0)
	for _, retentionDef := range strings.Split(retentionDefs, ",") {
		retentionDef = strings.TrimSpace(retentionDef)
		parts := strings.Split(retentionDef, ":")
		if len(parts) < 2 || len(parts) > 5 {
			return nil, fmt.Errorf("bad retentions spec %q", retentionDef)
		}

		// try old format
		val1, err1 := strconv.ParseInt(parts[0], 10, 0)
		val2, err2 := strconv.ParseInt(parts[1], 10, 0)

		var retention *whisper.Retention
		var err error
		if err1 == nil && err2 == nil {
			ret := whisper.NewRetention(int(val1), int(val2))
			retention = &ret
		} else {
			// try new format
			retention, err = whisper.ParseRetentionDef(retentionDef)
			if err != nil {
				return nil, err
			}
		}
		if len(parts) >= 3 {
			retention.ChunkSpan, err = dur.ParseUNsec(parts[2])
			if err != nil {
				return nil, err
			}
			if (Month_sec % retention.ChunkSpan) != 0 {
				return nil, errors.New("chunkSpan must fit without remainders into month_sec (28*24*60*60)")
			}
			_, ok := chunk.RevChunkSpans[retention.ChunkSpan]
			if !ok {
				return nil, fmt.Errorf("chunkSpan %s is not a valid value (https://github.com/raintank/metrictank/blob/master/docs/memory-server.md#valid-chunk-spans)", parts[2])
			}
		} else {
			// default to a valid chunkspan that can hold at least 100 points, or select the largest one otherwise.
			approxSpan := uint32(retention.SecondsPerPoint() * 100)
			var span uint32
			for _, span = range ChunkSpans {
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
	prevInterval := 0
	for _, r := range retentions {
		if r.SecondsPerPoint() <= prevInterval {
			// for users' own sanity, and also so we can reference to archive 0, 1, 2 and it means what you expect
			return nil, errors.New("aggregation archives must be in ascending order")
		}
		prevInterval = r.SecondsPerPoint()
	}
	return retentions, nil
}

// ReadWhisperSchemas reads and parses a storage-schemas.conf file and returns a sorted
// schemas structure
// see https://graphite.readthedocs.io/en/0.9.9/config-carbon.html#storage-schemas-conf
func ReadWhisperSchemas(file string) (WhisperSchemas, error) {
	config, err := configparser.Read(file)
	if err != nil {
		return nil, err
	}

	sections, err := config.AllSections()
	if err != nil {
		return nil, err
	}

	var schemas WhisperSchemas

	for i, sec := range sections {
		schema := Schema{}
		schema.Name =
			strings.Trim(strings.SplitN(sec.String(), "\n", 2)[0], " []")
		if schema.Name == "" || strings.HasPrefix(schema.Name, "#") {
			continue
		}

		patternStr := sec.ValueOf("pattern")
		if patternStr == "" {
			return nil, fmt.Errorf("[persister] Empty pattern for [%s]", schema.Name)
		}
		schema.Pattern, err = regexp.Compile(patternStr)
		if err != nil {
			return nil, fmt.Errorf("[persister] Failed to parse pattern %q for [%s]: %s",
				sec.ValueOf("pattern"), schema.Name, err.Error())
		}
		schema.RetentionStr = sec.ValueOf("retentions")
		schema.Retentions, err = ParseRetentionDefs(schema.RetentionStr)

		if err != nil {
			return nil, fmt.Errorf("[persister] Failed to parse retentions %q for [%s]: %s",
				sec.ValueOf("retentions"), schema.Name, err.Error())
		}

		priorityStr := sec.ValueOf("priority")

		p := int64(0)
		if priorityStr != "" {
			p, err = strconv.ParseInt(priorityStr, 10, 0)
			if err != nil {
				return nil, fmt.Errorf("[persister] Failed to parse priority %q for [%s]: %s", priorityStr, schema.Name, err)
			}
		}
		schema.Priority = int64(p)<<32 - int64(i) // to sort records with same priority by position in file

		schemas = append(schemas, schema)
	}

	sort.Sort(schemas)
	return schemas, nil
}
