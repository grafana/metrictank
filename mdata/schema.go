package mdata

import (
	"github.com/grafana/metrictank/conf"
)

func MaxChunkSpan() uint32 {
	return Schemas.MaxChunkSpan()
}

// TTLs returns the full set of unique TTLs (in seconds) used by the current schema config.
func TTLs() []uint32 {
	return Schemas.TTLs()
}

// MatchAgg returns the aggregation definition for the given metric key, and the index of it (to efficiently reference it)
// it will always find the aggregation definition because Aggregations has a catchall default
func MatchAgg(key string) (uint16, conf.Aggregation) {
	return Aggregations.Match(key)
}

// MatchSchema returns the schema for the given metric key, and the index of the schema (to efficiently reference it)
// it will always find the schema because Schemas has a catchall default
func MatchSchema(key string, interval int) (uint16, conf.Schema) {
	return Schemas.Match(key, interval)
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
