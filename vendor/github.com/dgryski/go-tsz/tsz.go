// Package tsz implement time-series compression
/*

http://www.vldb.org/pvldb/vol8/p1816-teller.pdf

*/
package tsz

import (
	"math"
	"sync"

	"github.com/dgryski/go-bits"
)

// Series is the basic series primitive
// you can concurrently put values, finish the stream, and create iterators
type Series struct {
	sync.Mutex

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

func New(t0 uint32) *Series {
	s := Series{
		T0:      t0,
		leading: ^uint8(0),
	}

	// block header
	s.bw.writeBits(uint64(t0), 32)

	return &s

}

func (s *Series) Bytes() []byte {
	s.Lock()
	defer s.Unlock()
	return s.bw.bytes()
}

func finish(w *bstream) {
	// write an end-of-stream record
	w.writeBits(0x0f, 4)
	w.writeBits(0xffffffff, 32)
	w.writeBit(zero)
}

func (s *Series) Finish() {
	s.Lock()
	if !s.finished {
		finish(&s.bw)
		s.finished = true
	}
	s.Unlock()
}

func (s *Series) Push(t uint32, v float64) {
	s.Lock()
	defer s.Unlock()

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

		leading := uint8(bits.Clz(vDelta))
		trailing := uint8(bits.Ctz(vDelta))

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

func (s *Series) Iter() *Iter {
	s.Lock()
	w := s.bw.clone()
	s.Unlock()

	finish(w)
	iter, _ := bstreamIterator(w)
	return iter
}

// Iter lets you iterate over a series.  It is not concurrency-safe.
type Iter struct {
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

func bstreamIterator(br *bstream) (*Iter, error) {

	br.count = 8

	t0, err := br.readBits(32)
	if err != nil {
		return nil, err
	}

	return &Iter{
		T0: uint32(t0),
		br: *br,
	}, nil
}

func NewIterator(b []byte) (*Iter, error) {
	return bstreamIterator(newBReader(b))
}

func (it *Iter) Next() bool {

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

		return true
	}

	// read delta-of-delta
	var d byte
	for i := 0; i < 4; i++ {
		d <<= 1
		bit, err := it.br.readBit()
		if err != nil {
			it.err = err
			return false
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
	case 0x02:
		sz = 7
	case 0x06:
		sz = 9
	case 0x0e:
		sz = 12
	case 0x0f:
		bits, err := it.br.readBits(32)
		if err != nil {
			it.err = err
			return false
		}

		// end of stream
		if bits == 0xffffffff {
			it.finished = true
			return false
		}

		dod = int32(bits)
	}

	if sz != 0 {
		bits, err := it.br.readBits(int(sz))
		if err != nil {
			it.err = err
			return false
		}
		if bits > (1 << (sz - 1)) {
			// or something
			bits = bits - (1 << sz)
		}
		dod = int32(bits)
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
		bit, err := it.br.readBit()
		if err != nil {
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

func (it *Iter) Values() (uint32, float64) {
	return it.t, it.val
}

func (it *Iter) Err() error {
	return it.err
}
