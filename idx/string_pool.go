package idx

import (
	"sync"
	"sync/atomic"
)

type Term struct {
	value    string
	refCount uint32
}

type StringPool struct {
	sync.RWMutex
	terms map[string]Term
}

func NewStringPool() *StringPool {
	return &StringPool{
		terms: make(map[string]Term),
	}
}

func (s *StringPool) Add(str string) string {
	if len(str) == 0 {
		return str
	}

	s.RLock()
	if t, ok := s.terms[str]; ok {
		s.RUnlock()
		atomic.AddUint32(&t.refCount, 1)
		return t.value
	}
	s.RUnlock()
	s.Lock()
	if t, ok := s.terms[str]; ok {
		s.Unlock()
		atomic.AddUint32(&t.refCount, 1)
		return t.value
	}
	s.terms[str] = Term{
		value:    str,
		refCount: 1,
	}
	s.Unlock()
	return str
}

func (s *StringPool) Del(str string) bool {
	var newCount uint32
	s.RLock()
	if t, ok := s.terms[str]; ok {
		s.RUnlock()
		newCount = atomic.AddUint32(&t.refCount, ^uint32(0))
	} else {
		s.RUnlock()
		return false
	}

	// no references to this term left, delete it from pool
	if newCount == 0 {
		s.Lock()
		delete(s.terms, str)
		s.Unlock()
	}

	return true
}
