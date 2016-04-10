package main

import "github.com/raintank/raintank-metric/metric_tank/iter"

type devnullStore struct {
}

func NewDevnullStore() *devnullStore {
	d := &devnullStore{}
	return d
}

func (c *devnullStore) Add(cwr *ChunkWriteRequest) {
}

func (c *devnullStore) Search(key string, start, end uint32) ([]iter.Iter, error) {
	return nil, nil
}

func (c *devnullStore) Stop() {
}
