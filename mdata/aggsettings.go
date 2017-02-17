package mdata

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/raintank/dur"
	"github.com/raintank/metrictank/mdata/chunk"
)

type AggSetting struct {
	Span      uint32 // in seconds, controls how many input points go into an aggregated point.
	ChunkSpan uint32 // duration of chunk of aggregated metric for storage, controls how many aggregated points go into 1 chunk
	NumChunks uint32 // number of chunks to keep in memory. remember, for a query from now until 3 months ago, we will end up querying the memory server as well.
	TTL       uint32 // how many seconds to keep the chunk in cassandra
	Ready     bool   // ready for reads?
}

type AggSettings struct {
	RawTTL uint32       // TTL for raw data
	Aggs   []AggSetting // aggregations
}

func NewAggSetting(span, chunkSpan, numChunks, ttl uint32, ready bool) AggSetting {
	return AggSetting{
		Span:      span,
		ChunkSpan: chunkSpan,
		NumChunks: numChunks,
		TTL:       ttl,
		Ready:     ready,
	}
}

type AggSettingsSpanAsc []AggSetting

func (a AggSettingsSpanAsc) Len() int           { return len(a) }
func (a AggSettingsSpanAsc) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a AggSettingsSpanAsc) Less(i, j int) bool { return a[i].Span < a[j].Span }

func ParseAggSettings(input string) ([]AggSetting, error) {
	set := strings.Split(input, ",")
	settings := make([]AggSetting, 0)
	for _, v := range set {
		if v == "" {
			continue
		}
		fields := strings.Split(v, ":")
		if len(fields) < 4 {
			return nil, errors.New("bad agg settings format")
		}
		aggSpan := dur.MustParseUNsec("aggsettings", fields[0])
		aggChunkSpan := dur.MustParseUNsec("aggsettings", fields[1])
		aggNumChunks := dur.MustParseUNsec("aggsettings", fields[2])
		aggTTL := dur.MustParseUNsec("aggsettings", fields[3])
		if (Month_sec % aggChunkSpan) != 0 {
			return nil, errors.New("aggChunkSpan must fit without remainders into month_sec (28*24*60*60)")
		}
		_, ok := chunk.RevChunkSpans[aggChunkSpan]
		if !ok {
			return nil, fmt.Errorf("aggChunkSpan %s is not a valid value (https://github.com/raintank/metrictank/blob/master/docs/memory-server.md#valid-chunk-spans)", fields[1])
		}
		ready := true
		var err error
		if len(fields) == 5 {
			ready, err = strconv.ParseBool(fields[4])
			if err != nil {
				return nil, fmt.Errorf("aggsettings ready: %s", err)
			}
		}
		settings = append(settings, NewAggSetting(aggSpan, aggChunkSpan, aggNumChunks, aggTTL, ready))
	}

	spanAsc := AggSettingsSpanAsc(settings)
	if !sort.IsSorted(spanAsc) {
		return nil, errors.New("aggregation settings need to be ordered by span, in ascending order")
	}

	return settings, nil
}
