package main

type Store interface {
	Add(cwr *ChunkWriteRequest)
	Search(key string, start, end uint32) ([]Iter, error)
	Stop()
}
