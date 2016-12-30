package mdata

import "github.com/raintank/metrictank/mdata/chunk"

type mockSearchResult struct {
	chunks []chunk.IterGen
	err    error
}

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
type mockStore struct {
	// index for the search results, pointing to which result will
	// be returned next
	CurrCall int
	// a list of results that will be returned by the Search() method
	Results []mockSearchResult
}

func NewMockStore() *mockStore {
	d := &mockStore{
		CurrCall: 0,
		Results:  make([]mockSearchResult, 0),
	}
	return d
}

// add a result to be returned on Search()
func (c *mockStore) AddMockResult(chunks []chunk.IterGen, err error) {
	// copy chunks because we don't want to modify the source
	chunksCopy := make([]chunk.IterGen, len(chunks))
	copy(chunksCopy, chunks)
	c.Results = append(c.Results, mockSearchResult{chunksCopy, err})
}

// flush and reset the mock
func (c *mockStore) ResetMock() {
	c.Results = c.Results[:0]
	c.CurrCall = 0
}

// currently that only exists to satisfy the interface
// might be extended to be useful in the future
func (c *mockStore) Add(cwr *ChunkWriteRequest) {
}

// returns the mock results, ignoring the search parameters
func (c *mockStore) Search(key string, start, end uint32) ([]chunk.IterGen, error) {
	if c.CurrCall < len(c.Results) {
		res := c.Results[c.CurrCall]
		c.CurrCall++
		return res.chunks, res.err
	}
	return make([]chunk.IterGen, 0), nil
}

func (c *mockStore) Stop() {
}
