package iter

import (
	"errors"

	"github.com/dgryski/go-tsz"
	"github.com/raintank/metrictank/mdata/chunk"
)

var (
	errUnknownChunkFormat = errors.New("unrecognized chunk format in cassandra")
)

type IterGen struct {
	b  []byte
	ts uint32
}

func NewGen(b []byte, ts uint32) (*IterGen, error) {
	if chunk.Format(b[0]) != chunk.FormatStandardGoTsz {
		return nil, errUnknownChunkFormat
	}

	return &IterGen{
		b,
		ts,
	}, nil
}

func (ig *IterGen) Get() (*Iter, error) {

	it, err := tsz.NewIterator(ig.b[1:])
	if err != nil {
		return nil, err
	}

	return &Iter{it}, nil
}

func (ig *IterGen) Size() uint64 {
	return uint64(len(ig.b))
}

func (ig IterGen) Ts() uint32 {
	return ig.ts
}
