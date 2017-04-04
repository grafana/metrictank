package mdata

import (
	"github.com/raintank/metrictank/conf"
)

func MaxChunkSpan() uint32 {
	return Schemas.MaxChunkSpan()
}

func TTLs() []uint32 {
	return Schemas.TTLs()
}

// MatchSchema returns the schema for the given metric key, and the index of the schema (to efficiently reference it)
// it will always find the schema because Schemas has a catchall default
func MatchSchema(key string, interval int) (uint16, conf.Schema) {
	return Schemas.Match(key, interval)
}

// MatchAgg returns the aggregation definition for the given metric key, and the index of it (to efficiently reference it)
// it will always find the aggregation definition because Aggregations has a catchall default
func MatchAgg(key string) (uint16, conf.Aggregation) {
	return Aggregations.Match(key)
}

func SetSingleSchema(ret ...conf.Retention) {
	Schemas = conf.NewSchemas(nil)
	Schemas.DefaultSchema.Retentions = conf.Retentions(ret)
	Schemas.BuildIndex()
}

func SetSingleAgg(met ...conf.Method) {
	Aggregations = conf.NewAggregations()
	Aggregations.DefaultAggregation.AggregationMethod = met
}
