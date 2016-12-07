package iter

import (
	"github.com/dgryski/go-tsz"
)

type IterGen struct {
	b  []byte
	ts uint32
}

func NewGen(b []byte, ts uint32) IterGen {
	return IterGen{
		b,
		ts,
	}
}

func (ig *IterGen) Get() (*Iter, error) {
	it, err := tsz.NewIterator(ig.b)
	if err != nil {
		return nil, err
	}
	return &Iter{it}, nil
}

func (ig *IterGen) Length() uint64 {
	return uint64(len(ig.b))
}

func (ig *IterGen) Ts() uint32 {
	return ig.ts
}
