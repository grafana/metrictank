package mdata

import (
	"context"

	schema "gopkg.in/raintank/schema.v1"

	"github.com/grafana/metrictank/consolidation"
	"github.com/grafana/metrictank/mdata/chunk"
	opentracing "github.com/opentracing/opentracing-go"
)

type Metrics interface {
	Get(key schema.MKey) (Metric, bool)
	GetOrCreate(key schema.MKey, schemaId, aggId uint16) Metric
}

type Metric interface {
	Add(ts uint32, val float64)
	Get(from, to uint32) Result
	GetAggregated(consolidator consolidation.Consolidator, aggSpan, from, to uint32) (Result, error)
}

type Store interface {
	Add(cwr *ChunkWriteRequest)
	Search(ctx context.Context, key schema.AMKey, ttl, start, end uint32) ([]chunk.IterGen, error)
	Stop()
	SetTracer(t opentracing.Tracer)
}
