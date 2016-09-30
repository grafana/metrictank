package util

import (
	"sync"
)

var BufferPool = sync.Pool{
	New: func() interface{} { return make([]byte, 0) },
}
