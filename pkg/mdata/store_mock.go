package mdata

import (
	"context"

	"github.com/grafana/metrictank/schema"

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
		intervalHint := cwr.Key.Archive.Span()
		itgen, err := chunk.NewIterGen(cwr.T0, intervalHint, cwr.Data)
		if err != nil {
			panic(err)
		}
		c.results[cwr.Key] = append(c.results[cwr.Key], itgen)
		c.items++
	}
}

// searches through the mock results and returns the right ones according to start / end
func (c *MockStore) Search(ctx context.Context, metric schema.AMKey, ttl, start, end uint32) ([]chunk.IterGen, error) {
	var itgens, res []chunk.IterGen
	var ok bool

	if itgens, ok = c.results[metric]; !ok {
		return res, nil
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
