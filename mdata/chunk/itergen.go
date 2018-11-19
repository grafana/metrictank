package chunk

import (
	"errors"

	"github.com/grafana/metrictank/mdata/chunk/tsz"
)

var (
	errUnknownChunkFormat = errors.New("unrecognized chunk format in cassandra")
	errUnknownSpanCode    = errors.New("corrupt data, chunk span code is not known")
	errShort              = errors.New("chunk is too short")
)

//go:generate msgp
type IterGen struct {
	T0           uint32
	intervalHint uint32 // a hint wrt expected alignment of points. useful to recover delta overflows in tsz.Series4h
	B            []byte
}

// NewBareIterGen creates an IterGen without validation
// note: it's ok for intervalHint to be 0 or 1 to mean unknown.
// it just means that series4h corruptions can't be remediated in single-point-per-chunk scenarios
func NewBareIterGen(t0, intervalHint uint32, b []byte) IterGen {
	return IterGen{t0, intervalHint, b}
}

// NewIterGen creates an IterGen and performs crude validation of the data
// note: it's ok for intervalHint to be 0 or 1 to mean unknown.
// it just means that series4h corruptions can't be remediated in single-point-per-chunk scenarios
func NewIterGen(t0, intervalHint uint32, b []byte) (IterGen, error) {
	switch Format(b[0]) {
	case FormatStandardGoTsz:
		if len(b) == 1 {
			return IterGen{}, errShort
		}
	case FormatStandardGoTszWithSpan, FormatGoTszLongWithSpan:
		if len(b) <= 2 {
			return IterGen{}, errShort
		}
		if int(b[1]) >= len(ChunkSpans) {
			return IterGen{}, errUnknownSpanCode
		}
	default:
		return IterGen{}, errUnknownChunkFormat
	}

	return IterGen{t0, intervalHint, b}, nil
}

func (ig IterGen) Format() Format {
	return Format(ig.B[0])
}

func (ig *IterGen) Get() (tsz.Iter, error) {
	// note: the tsz iterators modify the stream as they read it, so we must always give it a copy.
	switch ig.Format() {
	case FormatStandardGoTsz:
		src := ig.B[1:]
		dest := make([]byte, len(src))
		copy(dest, src)
		return tsz.NewIterator4h(dest, ig.intervalHint)
	case FormatStandardGoTszWithSpan:
		src := ig.B[2:]
		dest := make([]byte, len(src))
		copy(dest, src)
		return tsz.NewIterator4h(dest, ig.intervalHint)
	}
	// FormatGoTszLongWithSpan:
	src := ig.B[2:]
	dest := make([]byte, len(src))
	copy(dest, src)
	return tsz.NewIteratorLong(ig.T0, dest)
}

func (ig *IterGen) Span() uint32 {
	if Format(ig.B[0]) == FormatStandardGoTsz {
		return 0 // we don't know what the span is. sorry.
	}
	// already validated at IterGen creation time
	return ChunkSpans[SpanCode(ig.B[1])]
}

func (ig *IterGen) Size() uint64 { // TODO this is different than before. problem?
	return uint64(len(ig.B))
}

func (ig IterGen) Bytes() []byte { // TODO this is different than before. problem?
	return ig.B
}

// end of itergen (exclusive). next t0
func (ig IterGen) EndTs() uint32 {
	return ig.T0 + ig.Span()
}

//msgp:ignore IterGensAsc
type IterGensAsc []IterGen

func (a IterGensAsc) Len() int           { return len(a) }
func (a IterGensAsc) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a IterGensAsc) Less(i, j int) bool { return a[i].T0 < a[j].T0 }
