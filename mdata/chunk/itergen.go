package chunk

import (
	"errors"

	"github.com/dgryski/go-tsz"
)

var (
	errUnknownChunkFormat = errors.New("unrecognized chunk format in cassandra")
	errUnknownSpanCode    = errors.New("corrupt data, chunk span code is not known")
)

//go:generate msgp
type IterGen struct {
	B    []byte
	Ts   uint32
	Span uint32
}

func NewGen(b []byte, ts uint32) (*IterGen, error) {
	var span uint32 = 0

	switch Format(b[0]) {
	case FormatStandardGoTsz:
		b = b[1:]
	case FormatStandardGoTszWithSpan:
		if int(b[1]) >= len(ChunkSpans) {
			return nil, errUnknownSpanCode
		}
		span = ChunkSpans[SpanCode(b[1])]
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

func NewBareIterGen(b []byte, ts uint32, span uint32) *IterGen {
	return &IterGen{b, ts, span}
}

func (ig *IterGen) Get() (*Iter, error) {
	b := make([]byte, len(ig.B), len(ig.B))
	copy(b, ig.B)
	it, err := tsz.NewIterator(b)
	if err != nil {
		return nil, err
	}

	return &Iter{it}, nil
}

// end of itergen (exclusive)
func (ig IterGen) EndTs() uint32 {
	return ig.Ts + ig.Span
}

//msgp:ignore IterGensAsc
type IterGensAsc []IterGen

func (a IterGensAsc) Len() int           { return len(a) }
func (a IterGensAsc) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a IterGensAsc) Less(i, j int) bool { return a[i].Ts < a[j].Ts }
