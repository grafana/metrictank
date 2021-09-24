package tsz

import (
	"bytes"
	"encoding/binary"
	"io"
)

// bstream is a stream of bits
type bstream struct {
	// the data stream
	stream []byte

	// how many bits are valid in current byte
	count uint8
}

func newBReader(b []byte) *bstream {
	return &bstream{stream: b, count: 8}
}

func (b *bstream) clone() *bstream {
	d := make([]byte, len(b.stream))
	copy(d, b.stream)
	return &bstream{stream: d, count: b.count}
}

func (b *bstream) bytes() []byte {
	return b.stream
}

func (b *bstream) reset(stream []byte) {
	b.stream = stream
	b.count = 0
}

type bit bool

const (
	zero bit = false
	one  bit = true
)

func (b *bstream) writeBit(bit bit) {

	if b.count == 0 {
		b.stream = append(b.stream, 0)
		b.count = 8
	}

	i := len(b.stream) - 1

	if bit {
		b.stream[i] |= 1 << (b.count - 1)
	}

	b.count--
}

func (b *bstream) writeByte(byt byte) {

	if b.count == 0 {
		b.stream = append(b.stream, byt)
		return
	}

	i := len(b.stream) - 1

	// fill up b.b with b.count bits from byt
	b.stream[i] |= byt >> (8 - b.count)

	b.stream = append(b.stream, 0)
	i++
	b.stream[i] = byt << b.count
}

func (b *bstream) writeHighBits(in byte, nbits uint8) {
	if b.count == 0 {
		b.stream = append(b.stream, 0)
		b.count = 8
	}

	shiftBits := 8 - b.count
	partialByte := in >> shiftBits
	//fmt.Printf("Before: b.count = %d, nbits = %d, input = %064b, partialByte = %08b (dec=%d), lastByte = %08b (dec=%d)\n", b.count, nbits, u, partialByte, partialByte, b.stream[len(b.stream)-1], b.stream[len(b.stream)-1])
	b.stream[len(b.stream)-1] |= partialByte
	b.count -= nbits
}

// Writes as many bits as needed to finish a partial byte.
// This aligns further writes on byte boundaries, which is a little cheaper
func (b *bstream) alignByte(u uint64, nbits int) (uint64, int) {
	if b.count == 0 || b.count == 8 {
		// already aligned
		return u, nbits
	}

	writeBits := b.count
	if nbits < int(b.count) {
		writeBits = uint8(nbits)
	}
	b.writeHighBits(byte(u>>56), writeBits)

	return u << writeBits, nbits - int(writeBits)
}

func (b *bstream) writeBits(u uint64, nbits int) {
	u <<= (64 - uint(nbits))

	// For efficient byte at a time writing, first finish off any partial bytes we have
	u, nbits = b.alignByte(u, nbits)

	for nbits >= 8 {
		byt := byte(u >> 56)
		b.writeByte(byt)
		u <<= 8
		nbits -= 8
	}

	if nbits > 0 {
		b.writeHighBits(byte(u>>(56)), uint8(nbits))
	}
}

func (b *bstream) readBit() (bit, error) {

	if len(b.stream) == 0 {
		return false, io.EOF
	}

	if b.count == 0 {
		b.stream = b.stream[1:]
		// did we just run out of stuff to read?
		if len(b.stream) == 0 {
			return false, io.EOF
		}
		b.count = 8
	}

	b.count--
	d := b.stream[0] & 0x80
	b.stream[0] <<= 1
	return d != 0, nil
}

func (b *bstream) readByte() (byte, error) {

	if len(b.stream) == 0 {
		return 0, io.EOF
	}

	if b.count == 0 {
		b.stream = b.stream[1:]

		if len(b.stream) == 0 {
			return 0, io.EOF
		}

		b.count = 8
	}

	if b.count == 8 {
		b.count = 0
		return b.stream[0], nil
	}

	byt := b.stream[0]
	b.stream = b.stream[1:]

	if len(b.stream) == 0 {
		return 0, io.EOF
	}

	byt |= b.stream[0] >> b.count
	b.stream[0] <<= (8 - b.count)

	return byt, nil
}

func (b *bstream) readBits(nbits int) (uint64, error) {

	var u uint64

	for nbits >= 8 {
		byt, err := b.readByte()
		if err != nil {
			return 0, err
		}

		u = (u << 8) | uint64(byt)
		nbits -= 8
	}

	if nbits == 0 {
		return u, nil
	}

	if nbits > int(b.count) {
		u = (u << uint(b.count)) | uint64(b.stream[0]>>(8-b.count))
		nbits -= int(b.count)
		b.stream = b.stream[1:]

		if len(b.stream) == 0 {
			return 0, io.EOF
		}
		b.count = 8
	}

	u = (u << uint(nbits)) | uint64(b.stream[0]>>(8-uint(nbits)))
	b.stream[0] <<= uint(nbits)
	b.count -= uint8(nbits)
	return u, nil
}

// MarshalBinary implements the encoding.BinaryMarshaler interface
func (b *bstream) MarshalBinary() ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.BigEndian, b.count)
	if err != nil {
		return nil, err
	}
	err = binary.Write(buf, binary.BigEndian, b.stream)
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

// UnmarshalBinary implements the encoding.BinaryUnmarshaler interface
func (b *bstream) UnmarshalBinary(bIn []byte) error {
	buf := bytes.NewReader(bIn)
	err := binary.Read(buf, binary.BigEndian, &b.count)
	if err != nil {
		return err
	}
	b.stream = make([]byte, buf.Len())
	return binary.Read(buf, binary.BigEndian, &b.stream)
}
