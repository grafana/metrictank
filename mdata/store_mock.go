package mdata

import (
	"context"
	"errors"

	"github.com/raintank/schema"

	"github.com/grafana/metrictank/mdata/chunk"
	opentracing "github.com/opentracing/opentracing-go"
)

// MockStore is an in-memory Store implementation for unit tests
type MockStore struct {
	// the itgens to be searched and returned, indexed by metric
	results map[schema.AMKey][]chunk.IterGen
	// count of chunks in the store.
	items int
	// dont save any data.
	Drop bool
}

func NewMockStore() *MockStore {
	return &MockStore{
		results: make(map[schema.AMKey][]chunk.IterGen),
		Drop:    false,
	}
}

func (c *MockStore) Reset() {
	c.results = make(map[schema.AMKey][]chunk.IterGen)
	c.items = 0
}

func (c *MockStore) Items() int {
	return c.items
}

// Add adds a chunk to the store
func (c *MockStore) Add(cwr *ChunkWriteRequest) {
	if !c.Drop {
		var intervalHint uint32
		if cwr.Key.Archive > 0 {
			intervalHint = cwr.Key.Archive.Span()
		}
		itgen := chunk.NewBareIterGen(cwr.Chunk.SeriesLong.T0, intervalHint, cwr.Chunk.Encode(cwr.Span))
		c.results[cwr.Key] = append(c.results[cwr.Key], itgen)
		c.items++
	}
}

// searches through the mock results and returns the right ones according to start / end
func (c *MockStore) Search(ctx context.Context, metric schema.AMKey, ttl, start, end uint32) ([]chunk.IterGen, error) {
	var itgens, res []chunk.IterGen
	var ok bool

	if itgens, ok = c.results[metric]; !ok {
		return res, errors.New("metric not found")
	}

	for _, itgen := range itgens {
		// start is inclusive, end is exclusive
		if itgen.T0 < end && itgen.EndTs() > start && start < end {
			res = append(res, itgen)
		}
	}

	return res, nil
}

func (c *MockStore) Stop() {
}

func (c *MockStore) SetTracer(t opentracing.Tracer) {
}
