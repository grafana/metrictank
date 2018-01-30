package mdata

import (
	"context"
	"errors"

	"github.com/grafana/metrictank/mdata/chunk"
	opentracing "github.com/opentracing/opentracing-go"
)

// MockStore is an in-memory Store implementation for unit tests
type MockStore struct {
	// the itgens to be searched and returned, indexed by metric
	results map[string][]chunk.IterGen
}

func NewMockStore() *MockStore {
	return &MockStore{make(map[string][]chunk.IterGen)}
}

func (c *MockStore) ResetMock() {
	c.results = make(map[string][]chunk.IterGen)
}

// Add adds a chunk to the store
func (c *MockStore) Add(cwr *ChunkWriteRequest) {
	itgen := chunk.NewBareIterGen(cwr.Chunk.Series.Bytes(), cwr.Chunk.Series.T0, cwr.Span)
	c.results[cwr.Key] = append(c.results[cwr.Key], *itgen)
}

// searches through the mock results and returns the right ones according to start / end
func (c *MockStore) Search(ctx context.Context, metric string, ttl, start, end uint32) ([]chunk.IterGen, error) {
	var itgens []chunk.IterGen
	var ok bool
	res := make([]chunk.IterGen, 0)

	if itgens, ok = c.results[metric]; !ok {
		return res, errors.New("metric not found")
	}

	for _, itgen := range itgens {
		// start is inclusive, end is exclusive
		if itgen.Ts < end && itgen.EndTs() > start && start < end {
			res = append(res, itgen)
		}
	}

	return res, nil
}

func (c *MockStore) Stop() {
}

func (c *MockStore) SetTracer(t opentracing.Tracer) {
}
