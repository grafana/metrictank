package mdata

import "github.com/raintank/metrictank/mdata/chunk"

type mockSearchResult struct {
	chunks []chunk.IterGen
	err    error
}

type mockStore struct {
	CurrCall int
	Results  []mockSearchResult
}

func NewMockStore() *mockStore {
	d := &mockStore{
		CurrCall: 0,
		Results:  make([]mockSearchResult, 0),
	}
	return d
}

func (c *mockStore) AddMockResult(chunks []chunk.IterGen, err error) {
	// copy chunks because we don't want to modify the source
	chunksCopy := make([]chunk.IterGen, len(chunks))
	copy(chunksCopy, chunks)
	c.Results = append(c.Results, mockSearchResult{chunksCopy, err})
}

func (c *mockStore) ResetMock() {
	c.Results = c.Results[:0]
	c.CurrCall = 0
}

func (c *mockStore) Add(cwr *ChunkWriteRequest) {
}

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
