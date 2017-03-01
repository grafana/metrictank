package mdata

import (
	"regexp"

	"github.com/lomik/go-carbon/persister"
	"github.com/lomik/go-whisper"
	"github.com/raintank/metrictank/util"
)

// MatchSchema returns the schema for the given metric key, and the index of the schema (to efficiently reference it)
// it will always find the schema because we made sure there is a catchall '.*' pattern
func MatchSchema(key string) (uint16, persister.Schema) {
	i, schema, _ := Schemas.Match(key)
	return i, schema
}

// MatchAgg returns the aggregation definition for the given metric key, and the index of it (to efficiently reference it)
// i may be 1 more than the last defined by user, in which case it's the default.
func MatchAgg(key string) (uint16, persister.WhisperAggregationItem) {
	return Aggregations.Match(key)
}

// caller must assure i is valid
func GetRetentions(i uint16) whisper.Retentions {
	return Schemas[i].Retentions
}

// caller must assure i is valid
// note the special case
func GetAgg(i uint16) persister.WhisperAggregationItem {
	if i+1 > uint16(len(Aggregations.Data)) {
		return Aggregations.Default
	}
	return Aggregations.Data[i]
}

// TTLs returns a slice of all TTL's seen amongst all archives of all schemas
func TTLs() []uint32 {
	ttls := make(map[uint32]struct{})
	for _, s := range Schemas {
		for _, r := range s.Retentions {
			ttls[uint32(r.MaxRetention())] = struct{}{}
		}
	}
	var ttlSlice []uint32
	for ttl := range ttls {
		ttlSlice = append(ttlSlice, ttl)
	}
	return ttlSlice
}

// MaxChunkSpan returns the largest chunkspan seen amongst all archives of all schemas
func MaxChunkSpan() uint32 {
	max := uint32(0)
	for _, s := range Schemas {
		for _, r := range s.Retentions {
			max = util.Max(max, r.ChunkSpan)
		}
	}
	return max
}

func CommonAggregations() persister.WhisperAggregationItem {
	return persister.WhisperAggregationItem{
		AggregationMethod: []whisper.AggregationMethod{whisper.Average, whisper.Min, whisper.Max},
	}
}

func AllAggregations() persister.WhisperAggregationItem {
	return persister.WhisperAggregationItem{
		AggregationMethod: []whisper.AggregationMethod{whisper.Average, whisper.Min, whisper.Max, whisper.Sum, whisper.Last},
	}
}

func SetSingleSchema(ret ...whisper.Retention) {
	Schemas = persister.WhisperSchemas{
		{
			Pattern:    regexp.MustCompile(".*"),
			Retentions: whisper.Retentions(ret),
		},
	}
}

func SetOnlyDefaultAgg(met ...whisper.AggregationMethod) {
	Aggregations = persister.NewWhisperAggregation()
	Aggregations.Default.AggregationMethod = met
}
