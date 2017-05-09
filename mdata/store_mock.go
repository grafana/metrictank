package mdata

import (
	"errors"
	"github.com/raintank/metrictank/mdata/chunk"
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
	itgen := chunk.NewBareIterGen(cwr.chunk.Series.Bytes(), cwr.chunk.Series.T0, cwr.span)
	c.results[cwr.key] = append(c.results[cwr.key], *itgen)
}

// searches through the mock results and returns the right ones according to start / end
func (c *MockStore) Search(metric string, ttl, start, end uint32) ([]chunk.IterGen, error) {
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
