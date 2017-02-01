package mdata

import "github.com/raintank/metrictank/mdata/chunk"

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

func (c *devnullStore) Search(key string, ttl, start, end uint32) ([]chunk.IterGen, error) {
	return nil, nil
}

func (c *devnullStore) Stop() {
}
