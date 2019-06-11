package importer

import (
	"time"

	"github.com/grafana/metrictank/mdata"
	"github.com/raintank/schema"
)

//go:generate msgp

// ChunkWriteRequest is used by the importer utility to send cwrs over the network
// It does not contain the org id because this is assumed to be defined by the auth mechanism
type ChunkWriteRequest struct {
	mdata.ChunkWriteRequestPayload
	Archive schema.Archive
}

func NewChunkWriteRequest(archive schema.Archive, ttl, t0 uint32, data []byte, ts time.Time) ChunkWriteRequest {
	return ChunkWriteRequest{mdata.ChunkWriteRequestPayload{ttl, t0, data, ts}, archive}
}

func (c *ChunkWriteRequest) GetChunkWriteRequest(callback mdata.ChunkSaveCallback, key schema.MKey) mdata.ChunkWriteRequest {
	return mdata.ChunkWriteRequest{c.ChunkWriteRequestPayload, callback, schema.AMKey{MKey: key, Archive: c.Archive}}
}
