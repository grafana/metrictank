package mdata

import (
	"time"

	"github.com/raintank/schema"
)

// ChunkWriteRequest is a request to write a chunk into a store
type ChunkWriteRequest struct {
	Callback  func()
	Key       schema.AMKey
	TTL       uint32
	T0        uint32
	Data      []byte
	Timestamp time.Time
}

// NewChunkWriteRequest creates a new ChunkWriteRequest
func NewChunkWriteRequest(callback func(), key schema.AMKey, ttl, t0 uint32, data []byte, ts time.Time) ChunkWriteRequest {
	return ChunkWriteRequest{callback, key, ttl, t0, data, ts}
}
