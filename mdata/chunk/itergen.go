package chunk

import (
	"errors"

	"github.com/grafana/metrictank/mdata/chunk/tsz"
)

var (
	errUnknownChunkFormat = errors.New("unrecognized chunk format in cassandra")
	errUnknownSpanCode    = errors.New("corrupt data, chunk span code is not known")
)

//go:generate msgp
type IterGen struct {
	T0 uint32
	B  []byte
	Span uint32
}

func NewGen(t0 uint32, b []byte) (*IterGen, error) {
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
		t0,
		b,
		span,
	}, nil
}

func NewBareIterGen(t0 uint32, b []byte, span uint32) *IterGen {
	return &IterGen{t0, b, span}
}

func (ig *IterGen) Get() (*Iter, error) {
	b := make([]byte, len(ig.B), len(ig.B))
	copy(b, ig.B)
	it, err := tsz.NewIterator4h(b)
	if err != nil {
		return nil, err
	}

	return &Iter{it}, nil
}

func (ig *IterGen) Size() uint64 {
	return uint64(len(ig.B))
}

func (ig IterGen) Bytes() []byte {
	return ig.B
}

// end of itergen (exclusive)
func (ig IterGen) EndTs() uint32 {
	return ig.T0 + ig.Span()
}

// Encode encodes the itergen back into a chunk using the requested format.
// it is the callers responsibility to assure that when a format is chosen that
// encodes the span, the IterGen actually has a valid span
func (ig *IterGen) Encode(format Format) []byte {
	return encode(ig.Span, format, ig.B)
}

//msgp:ignore IterGensAsc
type IterGensAsc []IterGen

func (a IterGensAsc) Len() int           { return len(a) }
func (a IterGensAsc) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a IterGensAsc) Less(i, j int) bool { return a[i].T0 < a[j].T0 }
