package keycache

import (
	"sync"

	schema "github.com/grafana/metrictank/schema"
)

// SubKey is the last 15 bytes of a 16 byte Key
// We can track Key-identified metrics with a SubKey because
// we shard by the first byte of the Key.
type SubKey [15]byte

// Shard tracks which SubKey's have been seen since the last prune
type Shard struct {
	sync.Mutex
	data map[SubKey]struct{}
}

// NewShard creates a new shard
func NewShard() Shard {
	return Shard{
		data: make(map[SubKey]struct{}),
	}
}

// Touch marks the key as seen and returns whether it was seen before
func (s *Shard) Touch(key schema.Key) bool {
	var sub SubKey
	copy(sub[:], key[1:])
	s.Lock()
	_, ok := s.data[sub]
	s.data[sub] = struct{}{}
	s.Unlock()
	return ok
}

// Len returns the length of the shard
func (s *Shard) Len() int {
	s.Lock()
	l := len(s.data)
	s.Unlock()
	return l
}

// Reset resets the shard, making it empty
func (s *Shard) Reset() {
	s.Lock()
	s.data = make(map[SubKey]struct{})
	s.Unlock()
}
