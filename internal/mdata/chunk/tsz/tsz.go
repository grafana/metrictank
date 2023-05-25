// Package tsz implements time-series compression
// it is a fork of https://github.com/dgryski/go-tsz
// which implements http://www.vldb.org/pvldb/vol8/p1816-teller.pdf
// see devdocs/chunk-format.md for more info
package tsz

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"math/bits"
)

// Series4h is the basic series primitive. it is not concurrency safe.
// you shouldn't use it for chunks longer than 4.5 hours, due to overflow
// of the first delta (14 bits), though in some cases, the corresponding iterator
// can reconstruct the data. Only works for <=9h deltas/chunks though.
// See https://github.com/grafana/metrictank/pull/1126
type Series4h struct {
	// TODO(dgryski): timestamps in the paper are uint64
	T0  uint32
	t   uint32
	val float64

	bw       bstream
	leading  uint8
	trailing uint8
	finished bool

	tDelta uint32
}

// NewSeries4h creates a new Series4h
func NewSeries4h(t0 uint32) *Series4h {
	s := Series4h{
		T0:      t0,
		leading: ^uint8(0),
	}

	// block header
	s.bw.writeBits(uint64(t0), 32)

	return &s

}

// Bytes value of the series stream
func (s *Series4h) Bytes() []byte {
	return s.bw.bytes()
}

// Finish the series by writing an end-of-stream record
func (s *Series4h) Finish() {
	if !s.finished {
		finishV1(&s.bw)
		s.finished = true
	}
}

// Push a timestamp and value to the series
func (s *Series4h) Push(t uint32, v float64) {
	if s.t == 0 {
		// first point
		s.t = t
		s.val = v
		s.tDelta = t - s.T0
		s.bw.writeBits(uint64(s.tDelta), 14)
		s.bw.writeBits(math.Float64bits(v), 64)
		return
	}

	tDelta := t - s.t
	dod := int32(tDelta - s.tDelta)

	switch {
	case dod == 0:
		s.bw.writeBit(zero)
	case -63 <= dod && dod <= 64:
		s.bw.writeBits(0x02, 2) // '10'
		s.bw.writeBits(uint64(dod), 7)
	case -255 <= dod && dod <= 256:
		s.bw.writeBits(0x06, 3) // '110'
		s.bw.writeBits(uint64(dod), 9)
	case -2047 <= dod && dod <= 2048:
		s.bw.writeBits(0x0e, 4) // '1110'
		s.bw.writeBits(uint64(dod), 12)
	default:
		s.bw.writeBits(0x0f, 4) // '1111'
		s.bw.writeBits(uint64(dod), 32)
	}

	vDelta := math.Float64bits(v) ^ math.Float64bits(s.val)

	if vDelta == 0 {
		s.bw.writeBit(zero)
	} else {
		s.bw.writeBit(one)

		leading := uint8(bits.LeadingZeros64(vDelta))
		trailing := uint8(bits.TrailingZeros64(vDelta))

		// clamp number of leading zeros to avoid overflow when encoding
		if leading >= 32 {
			leading = 31
		}

		// TODO(dgryski): check if it's 'cheaper' to reset the leading/trailing bits instead
		if s.leading != ^uint8(0) && leading >= s.leading && trailing >= s.trailing {
			s.bw.writeBit(zero)
			s.bw.writeBits(vDelta>>s.trailing, 64-int(s.leading)-int(s.trailing))
		} else {
			s.leading, s.trailing = leading, trailing

			s.bw.writeBit(one)
			s.bw.writeBits(uint64(leading), 5)

			// Note that if leading == trailing == 0, then sigbits == 64.  But that value doesn't actually fit into the 6 bits we have.
			// Luckily, we never need to encode 0 significant bits, since that would put us in the other case (vdelta == 0).
			// So instead we write out a 0 and adjust it back to 64 on unpacking.
			sigbits := 64 - leading - trailing
			s.bw.writeBits(uint64(sigbits), 6)
			s.bw.writeBits(vDelta>>trailing, int(sigbits))
		}
	}

	s.tDelta = tDelta
	s.t = t
	s.val = v

}

// Iter4h lets you iterate over a series.  It is not concurrency-safe.
func (s *Series4h) Iter(intervalHint uint32) *Iter4h {
	w := s.bw.clone()

	finishV1(w)
	iter, _ := bstreamIterator4h(w, intervalHint)
	return iter
}

// Iter4h lets you iterate over a Series4h.  It is not concurrency-safe.
// For more info, see Series4h
type Iter4h struct {
	T0           uint32
	intervalHint uint32 // hint to recover corrupted delta's

	t   uint32
	val float64

	br       bstream
	leading  uint8
	trailing uint8

	finished bool

	tDelta uint32
	err    error
}

func bstreamIterator4h(br *bstream, intervalHint uint32) (*Iter4h, error) {

	br.count = 8

	t0, err := br.readBits(32)
	if err != nil {
		return nil, err
	}

	return &Iter4h{
		T0:           uint32(t0),
		intervalHint: intervalHint,
		br:           *br,
	}, nil
}

// NewIterator4h creates an Iter4h
func NewIterator4h(b []byte, intervalHint uint32) (*Iter4h, error) {
	return bstreamIterator4h(newBReader(b), intervalHint)
}

func (it *Iter4h) dod() (int32, bool) {
	var d byte
	for i := 0; i < 4; i++ {
		d <<= 1
		bit, err := it.br.readBit()
		if err != nil {
			it.err = err
			return 0, false
		}
		if bit == zero {
			break
		}
		d |= 1
	}

	var dod int32
	var sz uint
	switch d {
	case 0x00:
		// dod == 0
	case 0x02: // '10'
		sz = 7
	case 0x06: // '110'
		sz = 9
	case 0x0e: // '1110'
		sz = 12
	case 0x0f: // '1111'
		bits, err := it.br.readBits(32)
		if err != nil {
			it.err = err
			return 0, false
		}

		// end of stream
		if bits == 0xffffffff {
			it.finished = true
			return 0, false
		}

		dod = int32(bits)
	}

	if sz != 0 {
		bits, err := it.br.readBits(int(sz))
		if err != nil {
			it.err = err
			return 0, false
		}
		if bits > (1 << (sz - 1)) {
			// or something
			bits = bits - (1 << sz)
		}
		dod = int32(bits)
	}

	return dod, true
}

// Next iteration of the series iterator
func (it *Iter4h) Next() bool {

	if it.err != nil || it.finished {
		return false
	}

	if it.t == 0 {
		// read first t and v
		tDelta, err := it.br.readBits(14)
		if err != nil {
			it.err = err
			return false
		}
		it.tDelta = uint32(tDelta)
		it.t = it.T0 + it.tDelta
		v, err := it.br.readBits(64)
		if err != nil {
			it.err = err
			return false
		}

		it.val = math.Float64frombits(v)

		// look for delta overflow and remediate it
		// see https://github.com/grafana/metrictank/pull/1126 and
		// https://github.com/grafana/metrictank/pull/1129

		// first, let's try the hint based check
		// if we're aware of a consistent interval that the points should have
		// - which is the case for rollup archives: they have known, consistent intervals -
		// then we can simply rely on that to tell whether our delta overflowed.
		// this requires the interval to be >1 (always true for rollup chunks)
		// and not be a divisor of 16384 (also true for rollup chunks, they use long, round intervals like 300)
		if it.intervalHint > 0 && it.t%it.intervalHint != 0 {
			it.tDelta += 16384
			it.t += 16384
			return true
		}

		// if we don't have a hint - e.g. for raw data - read upcoming dod
		// if delta+dod <0 (aka the upcoming delta < 0),
		// our current delta overflowed, because points should always be in increasing time order
		// (have delta's > 0)
		// we must take a backup of the stream because reading from the stream reader modifies it.
		// note that potentially we could skip this remediation by using another hint: the chunkspan,
		// since we know the overflow cannot possibly happen for chunks <=4h in length. perhaps a future optimization.
		brBackup := it.br.clone()
		dod, ok := it.dod()
		if !ok {
			// this case should only happen if we're out of data (only a single point in the chunk)
			// in this case we can't know if the point is right or wrong.
			// so, nothing much to do in this case. return the possibly incorrect point.
			// though it is very unlikely to be wrong, because the overflow problem only tends to happen in long aggregated chunks
			// that have an intervalHint.
			// and for return value, stick to normal iter semantics:
			// this read succeeded, though we already know the next one will fail
			it.br = *brBackup
			return true
		}
		if dod+int32(tDelta) < 0 {
			it.tDelta += 16384
			it.t += 16384
		}
		it.br = *brBackup
		return true
	}

	// read delta-of-delta
	dod, ok := it.dod()
	if !ok {
		return false
	}

	tDelta := it.tDelta + uint32(dod)

	it.tDelta = tDelta
	it.t = it.t + it.tDelta

	// read compressed value
	bit, err := it.br.readBit()
	if err != nil {
		it.err = err
		return false
	}

	if bit == zero {
		// it.val = it.val
	} else {
		bit, itErr := it.br.readBit()
		if itErr != nil {
			it.err = err
			return false
		}
		if bit == zero {
			// reuse leading/trailing zero bits
			// it.leading, it.trailing = it.leading, it.trailing
		} else {
			bits, err := it.br.readBits(5)
			if err != nil {
				it.err = err
				return false
			}
			it.leading = uint8(bits)

			bits, err = it.br.readBits(6)
			if err != nil {
				it.err = err
				return false
			}
			mbits := uint8(bits)
			// 0 significant bits here means we overflowed and we actually need 64; see comment in encoder
			if mbits == 0 {
				mbits = 64
			}
			it.trailing = 64 - it.leading - mbits
		}

		mbits := int(64 - it.leading - it.trailing)
		bits, err := it.br.readBits(mbits)
		if err != nil {
			it.err = err
			return false
		}
		vbits := math.Float64bits(it.val)
		vbits ^= (bits << it.trailing)
		it.val = math.Float64frombits(vbits)
	}

	return true
}

// Values at the current iterator position
func (it *Iter4h) Values() (uint32, float64) {
	return it.t, it.val
}

// Err error at the current iterator position
func (it *Iter4h) Err() error {
	return it.err
}

type errMarshal struct {
	w   io.Writer
	r   io.Reader
	err error
}

func (em *errMarshal) write(t interface{}) {
	if em.err != nil {
		return
	}
	em.err = binary.Write(em.w, binary.BigEndian, t)
}

func (em *errMarshal) read(t interface{}) {
	if em.err != nil {
		return
	}
	em.err = binary.Read(em.r, binary.BigEndian, t)
}

// MarshalBinary implements the encoding.BinaryMarshaler interface
func (s *Series4h) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	em := &errMarshal{w: buf}
	em.write(s.T0)
	em.write(s.leading)
	em.write(s.t)
	em.write(s.tDelta)
	em.write(s.trailing)
	em.write(s.val)
	bStream, err := s.bw.MarshalBinary()
	if err != nil {
		return nil, err
	}
	em.write(bStream)
	if em.err != nil {
		return nil, em.err
	}
	return buf.Bytes(), nil
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface
func (s *Series4h) UnmarshalBinary(b []byte) error {
	buf := bytes.NewReader(b)
	em := &errMarshal{r: buf}
	em.read(&s.T0)
	em.read(&s.leading)
	em.read(&s.t)
	em.read(&s.tDelta)
	em.read(&s.trailing)
	em.read(&s.val)
	outBuf := make([]byte, buf.Len())
	em.read(outBuf)
	err := s.bw.UnmarshalBinary(outBuf)
	if err != nil {
		return err
	}
	if em.err != nil {
		return em.err
	}
	return nil
}
