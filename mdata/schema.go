package mdata

import (
	"github.com/raintank/metrictank/conf"
	"github.com/raintank/metrictank/util"
)

// TTLs returns a slice of all TTL's seen amongst all archives of all schemas
func TTLs() []uint32 {
	ttls := make(map[uint32]struct{})
	for _, s := range Schemas {
		for _, r := range s.Retentions {
			ttls[uint32(r.MaxRetention())] = struct{}{}
		}
	}
	for _, r := range conf.DefaultSchema.Retentions {
		ttls[uint32(r.MaxRetention())] = struct{}{}
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
	for _, r := range conf.DefaultSchema.Retentions {
		max = util.Max(max, r.ChunkSpan)
	}
	return max
}

// MatchSchema returns the schema for the given metric key, and the index of the schema (to efficiently reference it)
// it will always find the schema because Schemas has a catchall default
func MatchSchema(key string) (uint16, conf.Schema) {
	return Schemas.Match(key)
}

// MatchAgg returns the aggregation definition for the given metric key, and the index of it (to efficiently reference it)
// it will always find the aggregation definition because Aggregations has a catchall default
func MatchAgg(key string) (uint16, conf.Aggregation) {
	return Aggregations.Match(key)
}

func SetSingleSchema(ret ...conf.Retention) {
	Schemas = conf.NewSchemas()
	conf.DefaultSchema.Retentions = conf.Retentions(ret)
}

func SetSingleAgg(met ...conf.Method) {
	Aggregations = conf.NewAggregations()
	conf.DefaultAggregation.AggregationMethod = met
}
