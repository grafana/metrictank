package mdata

import (
	"context"
	"time"

	"github.com/grafana/metrictank/mdata/chunk"
)

type ChunkReadRequest struct {
	month     uint32
	sortKey   uint32
	q         string
	p         []interface{}
	timestamp time.Time
	out       chan outcome
	ctx       context.Context
}

type ChunkWriteRequest struct {
	Metric    *AggMetric
	Key       string
	Chunk     *chunk.Chunk
	TTL       uint32
	Timestamp time.Time
	Span      uint32
}

func NewChunkWriteRequest(metric *AggMetric, key string, chunk *chunk.Chunk, ttl, span uint32, ts time.Time) ChunkWriteRequest {
	return ChunkWriteRequest{metric, key, chunk, ttl, ts, span}
}
