package mdata

import (
	"time"

	"github.com/raintank/schema"
)

//go:generate msgp
//msgp:ignore ChunkWriteRequest

type ChunkSaveCallback func()

// ChunkWriteRequest is a request to write a chunk into a store
type ChunkWriteRequest struct {
	ChunkWriteRequestPayload
	Callback ChunkSaveCallback
	Key      schema.AMKey
}

func NewChunkWriteRequest(callback ChunkSaveCallback, key schema.AMKey, ttl, t0 uint32, data []byte, ts time.Time) ChunkWriteRequest {
	return ChunkWriteRequest{ChunkWriteRequestPayload{ttl, t0, data, ts}, callback, key}
}

type ChunkWriteRequestPayload struct {
	TTL       uint32
	T0        uint32
	Data      []byte
	Timestamp time.Time
}
