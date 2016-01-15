package main

import "time"

type ChunkWriteRequest struct {
	key       string
	chunk     *Chunk
	ttl       uint32
	timestamp time.Time
}
