package schema

// Code generated by github.com/tinylib/msgp DO NOT EDIT.

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *ControlMsg) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, err = dc.ReadMapHeader()
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "Defs":
			var zb0002 uint32
			zb0002, err = dc.ReadArrayHeader()
			if err != nil {
				err = msgp.WrapError(err, "Defs")
				return
			}
			if cap(z.Defs) >= int(zb0002) {
				z.Defs = (z.Defs)[:zb0002]
			} else {
				z.Defs = make([]MetricDefinition, zb0002)
			}
			for za0001 := range z.Defs {
				err = z.Defs[za0001].DecodeMsg(dc)
				if err != nil {
					err = msgp.WrapError(err, "Defs", za0001)
					return
				}
			}
		case "Op":
			{
				var zb0003 uint8
				zb0003, err = dc.ReadUint8()
				if err != nil {
					err = msgp.WrapError(err, "Op")
					return
				}
				z.Op = Operation(zb0003)
			}
		default:
			err = dc.Skip()
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z *ControlMsg) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 2
	// write "Defs"
	err = en.Append(0x82, 0xa4, 0x44, 0x65, 0x66, 0x73)
	if err != nil {
		return
	}
	err = en.WriteArrayHeader(uint32(len(z.Defs)))
	if err != nil {
		err = msgp.WrapError(err, "Defs")
		return
	}
	for za0001 := range z.Defs {
		err = z.Defs[za0001].EncodeMsg(en)
		if err != nil {
			err = msgp.WrapError(err, "Defs", za0001)
			return
		}
	}
	// write "Op"
	err = en.Append(0xa2, 0x4f, 0x70)
	if err != nil {
		return
	}
	err = en.WriteUint8(uint8(z.Op))
	if err != nil {
		err = msgp.WrapError(err, "Op")
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *ControlMsg) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 2
	// string "Defs"
	o = append(o, 0x82, 0xa4, 0x44, 0x65, 0x66, 0x73)
	o = msgp.AppendArrayHeader(o, uint32(len(z.Defs)))
	for za0001 := range z.Defs {
		o, err = z.Defs[za0001].MarshalMsg(o)
		if err != nil {
			err = msgp.WrapError(err, "Defs", za0001)
			return
		}
	}
	// string "Op"
	o = append(o, 0xa2, 0x4f, 0x70)
	o = msgp.AppendUint8(o, uint8(z.Op))
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *ControlMsg) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zb0001 uint32
	zb0001, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	for zb0001 > 0 {
		zb0001--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		switch msgp.UnsafeString(field) {
		case "Defs":
			var zb0002 uint32
			zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Defs")
				return
			}
			if cap(z.Defs) >= int(zb0002) {
				z.Defs = (z.Defs)[:zb0002]
			} else {
				z.Defs = make([]MetricDefinition, zb0002)
			}
			for za0001 := range z.Defs {
				bts, err = z.Defs[za0001].UnmarshalMsg(bts)
				if err != nil {
					err = msgp.WrapError(err, "Defs", za0001)
					return
				}
			}
		case "Op":
			{
				var zb0003 uint8
				zb0003, bts, err = msgp.ReadUint8Bytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "Op")
					return
				}
				z.Op = Operation(zb0003)
			}
		default:
			bts, err = msgp.Skip(bts)
			if err != nil {
				err = msgp.WrapError(err)
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *ControlMsg) Msgsize() (s int) {
	s = 1 + 5 + msgp.ArrayHeaderSize
	for za0001 := range z.Defs {
		s += z.Defs[za0001].Msgsize()
	}
	s += 3 + msgp.Uint8Size
	return
}

// DecodeMsg implements msgp.Decodable
func (z *Operation) DecodeMsg(dc *msgp.Reader) (err error) {
	{
		var zb0001 uint8
		zb0001, err = dc.ReadUint8()
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		(*z) = Operation(zb0001)
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z Operation) EncodeMsg(en *msgp.Writer) (err error) {
	err = en.WriteUint8(uint8(z))
	if err != nil {
		err = msgp.WrapError(err)
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z Operation) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	o = msgp.AppendUint8(o, uint8(z))
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Operation) UnmarshalMsg(bts []byte) (o []byte, err error) {
	{
		var zb0001 uint8
		zb0001, bts, err = msgp.ReadUint8Bytes(bts)
		if err != nil {
			err = msgp.WrapError(err)
			return
		}
		(*z) = Operation(zb0001)
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z Operation) Msgsize() (s int) {
	s = msgp.Uint8Size
	return
}
