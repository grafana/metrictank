package mdata

import (
	"context"

	"github.com/grafana/metrictank/mdata/chunk"
	opentracing "github.com/opentracing/opentracing-go"
)

type devnullStore struct {
	AddCount uint32
}

func NewDevnullStore() *devnullStore {
	d := &devnullStore{}
	return d
}

func (c *devnullStore) Add(cwr *ChunkWriteRequest) {
	c.AddCount++
}

func (c *devnullStore) Reset() {
	c.AddCount = 0
}

func (c *devnullStore) Search(ctx context.Context, key string, ttl, start, end uint32) ([]chunk.IterGen, error) {
	return nil, nil
}

func (c *devnullStore) Stop() {
}

func (c *devnullStore) SetTracer(t opentracing.Tracer) {
}
