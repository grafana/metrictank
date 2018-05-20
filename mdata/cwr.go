package mdata

import (
	"time"

	"github.com/grafana/metrictank/mdata/chunk"
	"gopkg.in/raintank/schema.v1"
)

// ChunkWriteRequest is a request to write a chunk into a store
type ChunkWriteRequest struct {
	Metric    *AggMetric
	Key       schema.AMKey
	Chunk     *chunk.Chunk
	TTL       uint32
	Timestamp time.Time
	Span      uint32
}

// NewChunkWriteRequest creates a new ChunkWriteRequest
func NewChunkWriteRequest(metric *AggMetric, key schema.AMKey, chunk *chunk.Chunk, ttl, span uint32, ts time.Time) ChunkWriteRequest {
	return ChunkWriteRequest{metric, key, chunk, ttl, ts, span}
}
