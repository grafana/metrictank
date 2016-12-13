package schema

// NOTE: THIS FILE WAS PRODUCED BY THE
// MSGP CODE GENERATION TOOL (github.com/tinylib/msgp)
// DO NOT EDIT

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *Point) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zxvk uint32
	zxvk, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zxvk > 0 {
		zxvk--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "Val":
			z.Val, err = dc.ReadFloat64()
			if err != nil {
				return
			}
		case "Ts":
			z.Ts, err = dc.ReadUint32()
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
func (z Point) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 2
	// write "Val"
	err = en.Append(0x82, 0xa3, 0x56, 0x61, 0x6c)
	if err != nil {
		return err
	}
	err = en.WriteFloat64(z.Val)
	if err != nil {
		return
	}
	// write "Ts"
	err = en.Append(0xa2, 0x54, 0x73)
	if err != nil {
		return err
	}
	err = en.WriteUint32(z.Ts)
	if err != nil {
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z Point) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 2
	// string "Val"
	o = append(o, 0x82, 0xa3, 0x56, 0x61, 0x6c)
	o = msgp.AppendFloat64(o, z.Val)
	// string "Ts"
	o = append(o, 0xa2, 0x54, 0x73)
	o = msgp.AppendUint32(o, z.Ts)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Point) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zbzg uint32
	zbzg, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		return
	}
	for zbzg > 0 {
		zbzg--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "Val":
			z.Val, bts, err = msgp.ReadFloat64Bytes(bts)
			if err != nil {
				return
			}
		case "Ts":
			z.Ts, bts, err = msgp.ReadUint32Bytes(bts)
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
func (z Point) Msgsize() (s int) {
	s = 1 + 4 + msgp.Float64Size + 3 + msgp.Uint32Size
	return
}
