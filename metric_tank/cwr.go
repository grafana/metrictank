package main

import "time"

type ChunkReadRequest struct {
	month   uint32
	sortKey uint32
	q       string
	p       []interface{}
	out     chan outcome
}

type ChunkWriteRequest struct {
	key       string
	chunk     *Chunk
	ttl       uint32
	timestamp time.Time
}
