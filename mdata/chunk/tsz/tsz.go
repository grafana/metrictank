// Package tsz implements time-series compression
// it is a fork of https://github.com/dgryski/go-tsz
// which implements http://www.vldb.org/pvldb/vol8/p1816-teller.pdf
// see devdocs/gorilla-compression.md for more info
package tsz

import (
	"bytes"
	"encoding/binary"
	"io"
	"math"
	"math/bits"
	"sync"
)

// Series4h is the basic series primitive
// you can concurrently put values, finish the stream, and create iterators
// you shouldn't use it for chunks longer than 4.5 hours, due to overflow
// of the first delta (14 bits), though in some cases, the corresponding iterator
// can reconstruct the data. Only works for <=9h deltas/chunks though.
// See https://github.com/grafana/metrictank/pull/1126
type Series4h struct {
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

// Finish the series by writing an end-of-stream record
func (s *Series4h) Finish() {
	s.Lock()
	if !s.finished {
		finish(&s.bw)
		s.finished = true
	}
	s.Unlock()
}

// Push a timestamp and value to the series
func (s *Series4h) Push(t uint32, v float64) {
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
func (s *Series4h) Iter() *Iter4h {
	s.Lock()
	w := s.bw.clone()
	s.Unlock()

	finish(w)
	iter, _ := bstreamIterator4h(w)
	return iter
}

// Iter4h lets you iterate over a Series4h.  It is not concurrency-safe.
// For more info, see Series4h
type Iter4h struct {
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

func bstreamIterator4h(br *bstream) (*Iter4h, error) {

	br.count = 8

	t0, err := br.readBits(32)
	if err != nil {
		return nil, err
	}

	return &Iter4h{
		T0: uint32(t0),
		br: *br,
	}, nil
}

// NewIterator4h creates an Iter4h
func NewIterator4h(b []byte) (*Iter4h, error) {
	return bstreamIterator4h(newBReader(b))
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

		// special case: read upcoming dod
		// if delta+dod <0 (aka the upcoming delta < 0),
		// our current delta overflowed, and should rectify it.
		// see https://github.com/grafana/metrictank/pull/1126
		// but we must take a backup of the stream because reading from the
		// stream modifies it.
		brBackup := it.br.clone()
		dod, ok := it.dod()
		if !ok {
			// in this case we can't know if the point is right or wrong.
			// long chunks with a single point in them may lead to a wrong read, but this should be rare.
			// we can't just adjust the timestamp because we don't know the length of the chunk
			// (though this could be done by having the caller pass us that information), nor whether
			// a delta that may seem low compared to chunk length was intentional or not.
			// so, nothing much to do in this case. return the possibly incorrect point.
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
