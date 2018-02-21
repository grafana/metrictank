package mdata

import (
	"context"

	"github.com/grafana/metrictank/consolidation"
	"github.com/grafana/metrictank/mdata/chunk"
	opentracing "github.com/opentracing/opentracing-go"
	"gopkg.in/raintank/schema.v1"
)

type Metrics interface {
	Get(key string) (Metric, bool)
	StoreDataPoint(metric schema.DataPoint, partition int32)
	LoadOrStore(key string, metric Metric) (Metric, bool)
	Store(key string, metric Metric)
	GC()
}

type Metric interface {
	Add(point schema.DataPoint, partition int32)
	Get(from, to uint32) Result
	GetAggregated(consolidator consolidation.Consolidator, aggSpan, from, to uint32) (Result, error)
	SyncChunkSaveState(ts uint32, consolidator consolidation.Consolidator, aggSpan uint32)
	GC(chunkMinTs, metricMinTs uint32) bool
}

type Store interface {
	Add(cwr *ChunkWriteRequest)
	Search(ctx context.Context, key string, ttl, start, end uint32) ([]chunk.IterGen, error)
	Stop()
	SetTracer(t opentracing.Tracer)
}
