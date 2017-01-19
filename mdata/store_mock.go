package mdata

import (
	"errors"
	"github.com/raintank/metrictank/mdata/chunk"
)

// a data store that satisfies the interface `mdata.Store`.
//
// it is intended to be used in unit tests where it is necessary
// that the backend store returns values, but we don't want to
// involve a real store like for example Cassandra.
// the mockstore simply returns the results it has gotten added
// via the AddMockResult() method.
// this can be extended if future unit tests require the mock
// store to be smarter, or for example if they require it to
// keep what has been passed into Add().
type MockStore struct {
	// the itgens to be searched and returned, indexed by metric
	results map[string][]chunk.IterGen
}

func NewMockStore() *MockStore {
	return &MockStore{make(map[string][]chunk.IterGen)}
}

// add a chunk to be returned on Search()
func (c *MockStore) AddMockResult(metric string, itgen chunk.IterGen) {
	if itgens, ok := c.results[metric]; !ok {
		itgens = make([]chunk.IterGen, 0)
		c.results[metric] = itgens
	}

	c.results[metric] = append(c.results[metric], itgen)
}

func (c *MockStore) ResetMock() {
	c.results = make(map[string][]chunk.IterGen)
}

// currently that only exists to satisfy the interface
// might be extended to be useful in the future
func (c *MockStore) Add(cwr *ChunkWriteRequest) {
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
		if itgen.Ts() < end && itgen.EndTs() > start && start < end {
			res = append(res, itgen)
		}
	}

	return res, nil
}

func (c *MockStore) Stop() {
}
