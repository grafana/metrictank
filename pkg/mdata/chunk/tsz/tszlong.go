// Package tsz implements time-series compression
// it is a fork of https://github.com/dgryski/go-tsz
// which implements http://www.vldb.org/pvldb/vol8/p1816-teller.pdf
// see devdocs/chunk-format.md for more info
package tsz

import (
	"bytes"
	"math"
	"math/bits"
)

// SeriesLong similar to Series4h, except:
// * it doesn't write t0 to the stream (for callers that track t0 corresponding to a chunk separately)
// * it doesn't store an initial delta. instead, it assumes a starting delta of 60 and uses delta-of-delta
//   encoding from the get-go.
// * it uses a more compact way to mark end-of-stream
type SeriesLong struct {
	// TODO(dgryski): timestamps in the paper are uint64
	T0  uint32 // exposed for caller convenience. do NOT set directly. set via constructor
	T   uint32 // exposed for caller convenience. do NOT set directly. may only be set via Push()
	val float64

	bw       bstream
	leading  uint8
	trailing uint8
	Finished bool // exposed for caller convenience. do NOT set directly.

	tDelta uint32
}

// New series
func NewSeriesLong(t0 uint32) *SeriesLong {
	s := SeriesLong{
		T0:      t0,
		leading: ^uint8(0),
		tDelta:  60,
	}
	return &s

}

// Bytes value of the series stream
func (s *SeriesLong) Bytes() []byte {
	return s.bw.bytes()
}

// Finish the series by writing an end-of-stream record
func (s *SeriesLong) Finish() {
	if !s.Finished {
		finishV2(&s.bw)
		s.Finished = true
	}
}

// Push a timestamp and value to the series
func (s *SeriesLong) Push(t uint32, v float64) {
	var first bool

	tDelta := t - s.T
	if s.T == 0 {
		first = true
		tDelta = t - s.T0
	}
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
		s.bw.writeBits(0x1e, 5) // '11110'
		s.bw.writeBits(uint64(dod), 32)
	}

	s.tDelta = tDelta
	s.T = t

	if first {
		// first point; write full float value
		s.bw.writeBits(math.Float64bits(v), 64)
		s.val = v
		return
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

		sigbits := 64 - leading - trailing
		currbits := 64 - s.leading - s.trailing

		// We have 2 cases here:
		// 1. We have at least as many leading and trailing zeroes than we previously did
		// 2. We have fewer leading or trailing zeroes than previously
		// In the case of number 2, we need to re-encode the leading/trailing zeroes count to be able to handle the number
		// of significant bits we need to write.
		// In the case of number 1, we have more options. If we have significantly more leading/trailing zeroes, then we
		// might be wasting a lot of bits by writing more significant bits than we need. In this case we *could* choose
		// to "reset" by writing out the count of significant bits. It costs 11 bits to reset, so ideally we would only
		// reset when we know we will waste more bits than it would cost to reset. Add another 11 bits in case this was
		// an outlier and we need to 'reset' back to more significant bits.
		if s.leading != ^uint8(0) && leading >= s.leading && trailing >= s.trailing && currbits-sigbits < 22 {
			// Case 1: Keep the previous leading/trailing zeroes
			s.bw.writeBit(zero)
			s.bw.writeBits(vDelta>>s.trailing, int(currbits))
		} else {
			// Case 2: Reset leading/trailing zeroes
			s.leading, s.trailing = leading, trailing

			s.bw.writeBit(one)
			s.bw.writeBits(uint64(leading), 5)

			// Note that if leading == trailing == 0, then sigbits == 64.  But that value doesn't actually fit into the 6 bits we have.
			// Luckily, we never need to encode 0 significant bits, since that would put us in the other case (vdelta == 0).
			// So instead we write out a 0 and adjust it back to 64 on unpacking.
			s.bw.writeBits(uint64(sigbits), 6)
			s.bw.writeBits(vDelta>>trailing, int(sigbits))
		}
	}

	s.val = v

}

// IterLong lets you iterate over a series.  It is not concurrency-safe.
func (s *SeriesLong) Iter() *IterLong {
	w := s.bw.clone()

	finishV2(w)
	iter, _ := bstreamIteratorLong(s.T0, w)
	return iter
}

// IterLong lets you iterate over a series.  It is not concurrency-safe.
type IterLong struct {
	T0 uint32

	t   uint32
	val float64

	br       bstream
	leading  uint8
	trailing uint8

	finished bool

	tDelta uint32
	err    error
}

func bstreamIteratorLong(t0 uint32, br *bstream) (*IterLong, error) {

	br.count = 8

	return &IterLong{
		T0:     t0,
		br:     *br,
		tDelta: 60,
	}, nil
}

// NewIteratorLong for the series
func NewIteratorLong(t0 uint32, b []byte) (*IterLong, error) {
	return bstreamIteratorLong(t0, newBReader(b))
}

func (it *IterLong) dod() (int32, bool) {
	var d byte
	for i := 0; i < 5; i++ {
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
	case 0x1e: // '11110'
		bits, err := it.br.readBits(32)
		if err != nil {
			it.err = err
			return 0, false
		}
		dod = int32(bits)
	case 0x1f: // '11111': end-of-stream
		it.finished = true
		return 0, false
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
func (it *IterLong) Next() bool {

	if it.err != nil || it.finished {
		return false
	}

	var first bool
	if it.t == 0 {
		it.t = it.T0
		first = true
	}

	// read delta-of-delta
	dod, ok := it.dod()
	if !ok {
		return false
	}

	it.tDelta += uint32(dod)
	it.t = it.t + it.tDelta

	if first {
		// first point. read the float raw
		v, err := it.br.readBits(64)
		if err != nil {
			it.err = err
			return false
		}

		it.val = math.Float64frombits(v)
		return true
	}

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
func (it *IterLong) Values() (uint32, float64) {
	return it.t, it.val
}

// Err error at the current iterator position
func (it *IterLong) Err() error {
	return it.err
}

// MarshalBinary implements the encoding.BinaryMarshaler interface
func (s *SeriesLong) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	em := &errMarshal{w: buf}
	em.write(s.T0)
	em.write(s.leading)
	em.write(s.T)
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
func (s *SeriesLong) UnmarshalBinary(b []byte) error {
	buf := bytes.NewReader(b)
	em := &errMarshal{r: buf}
	em.read(&s.T0)
	em.read(&s.leading)
	em.read(&s.T)
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
