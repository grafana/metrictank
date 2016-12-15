package iter

import (
	"errors"

	"github.com/dgryski/go-tsz"
	"github.com/raintank/metrictank/mdata/chunk"
)

var (
	errUnknownChunkFormat = errors.New("unrecognized chunk format in cassandra")
	errUnknownSpanCode    = errors.New("corrupt data, chunk span code is not known")
)

type IterGen struct {
	b    []byte
	ts   uint32
	span uint32
}

func NewGen(b []byte, ts uint32) (*IterGen, error) {
	var span uint32 = 0

	switch chunk.Format(b[0]) {
	case chunk.FormatStandardGoTsz:
		b = b[1:]
	case chunk.FormatStandardGoTszWithSpan:
		if int(b[1]) >= len(chunk.ChunkSpans) {
			return nil, errUnknownSpanCode
		}
		span = chunk.ChunkSpans[chunk.SpanCode(b[1])]
		b = b[2:]
	default:
		return nil, errUnknownChunkFormat
	}

	return &IterGen{
		b,
		ts,
		span,
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

func (ig IterGen) Span() uint32 {
	return ig.span
}
