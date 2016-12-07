package iter

import (
	"github.com/dgryski/go-tsz"
	"github.com/raintank/worldping-api/pkg/log"
)

type IterGen struct {
	b   []byte
	ts  uint32
	len uint64
}

func NewGen(b []byte, ts uint32, len uint64) IterGen {
	return IterGen{
		b,
		ts,
		len,
	}
}

func (ig *IterGen) Get() (*Iter, error) {
	it, err := tsz.NewIterator(ig.b)
	if err != nil {
		log.Error(3, "failed to unpack cassandra payload. %s", err)
		return nil, err
	}
	return &Iter{it}, nil
}

func (ig *IterGen) Length() uint64 {
	return ig.len
}

func (ig *IterGen) Ts() uint32 {
	return ig.ts
}
