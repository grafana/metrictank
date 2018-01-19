package chunk

// NOTE: THIS FILE WAS PRODUCED BY THE
// MSGP CODE GENERATION TOOL (github.com/tinylib/msgp)
// DO NOT EDIT

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *IterGen) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "B":
			z.B, err = dc.ReadBytes(z.B)
			if err != nil {
				return
			}
		case "Ts":
			z.Ts, err = dc.ReadUint32()
			if err != nil {
				return
			}
		case "Span":
			z.Span, err = dc.ReadUint32()
			if err != nil {
				return
			}
		default:
			err = dc.Skip()
			if err != nil {
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *IterGen) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 3
	// write "B"
	err = en.Append(0x83, 0xa1, 0x42)
	if err != nil {
		return
	}
	err = en.WriteBytes(z.B)
	if err != nil {
		return
	}
	// write "Ts"
	err = en.Append(0xa2, 0x54, 0x73)
	if err != nil {
		return
	}
	err = en.WriteUint32(z.Ts)
	if err != nil {
		return
	}
	// write "Span"
	err = en.Append(0xa4, 0x53, 0x70, 0x61, 0x6e)
	if err != nil {
		return
	}
	err = en.WriteUint32(z.Span)
	if err != nil {
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *IterGen) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 3
	// string "B"
	o = append(o, 0x83, 0xa1, 0x42)
	o = msgp.AppendBytes(o, z.B)
	// string "Ts"
	o = append(o, 0xa2, 0x54, 0x73)
	o = msgp.AppendUint32(o, z.Ts)
	// string "Span"
	o = append(o, 0xa4, 0x53, 0x70, 0x61, 0x6e)
	o = msgp.AppendUint32(o, z.Span)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *IterGen) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "B":
			z.B, bts, err = msgp.ReadBytesBytes(bts, z.B)
			if err != nil {
				return
			}
		case "Ts":
			z.Ts, bts, err = msgp.ReadUint32Bytes(bts)
			if err != nil {
				return
			}
		case "Span":
			z.Span, bts, err = msgp.ReadUint32Bytes(bts)
			if err != nil {
				return
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *IterGen) Msgsize() (s int) {
	s = 1 + 2 + msgp.BytesPrefixSize + len(z.B) + 3 + msgp.Uint32Size + 5 + msgp.Uint32Size
	return
}
