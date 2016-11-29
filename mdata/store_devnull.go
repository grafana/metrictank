package mdata

import "github.com/raintank/metrictank/iter"

type devnullStore struct {
}

func NewDevnullStore() *devnullStore {
	d := &devnullStore{}
	return d
}

func (c *devnullStore) Add(cwr *ChunkWriteRequest) {
}

func (c *devnullStore) Search(key string, start, end uint32) ([]iter.IterGen, error) {
	return nil, nil
}

func (c *devnullStore) Stop() {
}
