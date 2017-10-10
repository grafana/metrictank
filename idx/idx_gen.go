package idx

// NOTE: THIS FILE WAS PRODUCED BY THE
// MSGP CODE GENERATION TOOL (github.com/tinylib/msgp)
// DO NOT EDIT

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *Archive) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "MetricDefinition":
			err = z.MetricDefinition.DecodeMsg(dc)
			if err != nil {
				return
			}
		case "SchemaId":
			z.SchemaId, err = dc.ReadUint16()
			if err != nil {
				return
			}
		case "AggId":
			z.AggId, err = dc.ReadUint16()
			if err != nil {
				return
			}
		case "LastSave":
			z.LastSave, err = dc.ReadUint32()
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
func (z *Archive) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 4
	// write "MetricDefinition"
	err = en.Append(0x84, 0xb0, 0x4d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x44, 0x65, 0x66, 0x69, 0x6e, 0x69, 0x74, 0x69, 0x6f, 0x6e)
	if err != nil {
		return err
	}
	err = z.MetricDefinition.EncodeMsg(en)
	if err != nil {
		return
	}
	// write "SchemaId"
	err = en.Append(0xa8, 0x53, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x49, 0x64)
	if err != nil {
		return err
	}
	err = en.WriteUint16(z.SchemaId)
	if err != nil {
		return
	}
	// write "AggId"
	err = en.Append(0xa5, 0x41, 0x67, 0x67, 0x49, 0x64)
	if err != nil {
		return err
	}
	err = en.WriteUint16(z.AggId)
	if err != nil {
		return
	}
	// write "LastSave"
	err = en.Append(0xa8, 0x4c, 0x61, 0x73, 0x74, 0x53, 0x61, 0x76, 0x65)
	if err != nil {
		return err
	}
	err = en.WriteUint32(z.LastSave)
	if err != nil {
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *Archive) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 4
	// string "MetricDefinition"
	o = append(o, 0x84, 0xb0, 0x4d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x44, 0x65, 0x66, 0x69, 0x6e, 0x69, 0x74, 0x69, 0x6f, 0x6e)
	o, err = z.MetricDefinition.MarshalMsg(o)
	if err != nil {
		return
	}
	// string "SchemaId"
	o = append(o, 0xa8, 0x53, 0x63, 0x68, 0x65, 0x6d, 0x61, 0x49, 0x64)
	o = msgp.AppendUint16(o, z.SchemaId)
	// string "AggId"
	o = append(o, 0xa5, 0x41, 0x67, 0x67, 0x49, 0x64)
	o = msgp.AppendUint16(o, z.AggId)
	// string "LastSave"
	o = append(o, 0xa8, 0x4c, 0x61, 0x73, 0x74, 0x53, 0x61, 0x76, 0x65)
	o = msgp.AppendUint32(o, z.LastSave)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Archive) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "MetricDefinition":
			bts, err = z.MetricDefinition.UnmarshalMsg(bts)
			if err != nil {
				return
			}
		case "SchemaId":
			z.SchemaId, bts, err = msgp.ReadUint16Bytes(bts)
			if err != nil {
				return
			}
		case "AggId":
			z.AggId, bts, err = msgp.ReadUint16Bytes(bts)
			if err != nil {
				return
			}
		case "LastSave":
			z.LastSave, bts, err = msgp.ReadUint32Bytes(bts)
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
func (z *Archive) Msgsize() (s int) {
	s = 1 + 17 + z.MetricDefinition.Msgsize() + 9 + msgp.Uint16Size + 6 + msgp.Uint16Size + 9 + msgp.Uint32Size
	return
}

// DecodeMsg implements msgp.Decodable
func (z *Node) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zcmr uint32
	zcmr, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zcmr > 0 {
		zcmr--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "Path":
			z.Path, err = dc.ReadString()
			if err != nil {
				return
			}
		case "Leaf":
			z.Leaf, err = dc.ReadBool()
			if err != nil {
				return
			}
		case "Defs":
			var zajw uint32
			zajw, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.Defs) >= int(zajw) {
				z.Defs = (z.Defs)[:zajw]
			} else {
				z.Defs = make([]Archive, zajw)
			}
			for zbai := range z.Defs {
				err = z.Defs[zbai].DecodeMsg(dc)
				if err != nil {
					return
				}
			}
		case "HasChildren":
			z.HasChildren, err = dc.ReadBool()
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
func (z *Node) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 4
	// write "Path"
	err = en.Append(0x84, 0xa4, 0x50, 0x61, 0x74, 0x68)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Path)
	if err != nil {
		return
	}
	// write "Leaf"
	err = en.Append(0xa4, 0x4c, 0x65, 0x61, 0x66)
	if err != nil {
		return err
	}
	err = en.WriteBool(z.Leaf)
	if err != nil {
		return
	}
	// write "Defs"
	err = en.Append(0xa4, 0x44, 0x65, 0x66, 0x73)
	if err != nil {
		return err
	}
	err = en.WriteArrayHeader(uint32(len(z.Defs)))
	if err != nil {
		return
	}
	for zbai := range z.Defs {
		err = z.Defs[zbai].EncodeMsg(en)
		if err != nil {
			return
		}
	}
	// write "HasChildren"
	err = en.Append(0xab, 0x48, 0x61, 0x73, 0x43, 0x68, 0x69, 0x6c, 0x64, 0x72, 0x65, 0x6e)
	if err != nil {
		return err
	}
	err = en.WriteBool(z.HasChildren)
	if err != nil {
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *Node) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 4
	// string "Path"
	o = append(o, 0x84, 0xa4, 0x50, 0x61, 0x74, 0x68)
	o = msgp.AppendString(o, z.Path)
	// string "Leaf"
	o = append(o, 0xa4, 0x4c, 0x65, 0x61, 0x66)
	o = msgp.AppendBool(o, z.Leaf)
	// string "Defs"
	o = append(o, 0xa4, 0x44, 0x65, 0x66, 0x73)
	o = msgp.AppendArrayHeader(o, uint32(len(z.Defs)))
	for zbai := range z.Defs {
		o, err = z.Defs[zbai].MarshalMsg(o)
		if err != nil {
			return
		}
	}
	// string "HasChildren"
	o = append(o, 0xab, 0x48, 0x61, 0x73, 0x43, 0x68, 0x69, 0x6c, 0x64, 0x72, 0x65, 0x6e)
	o = msgp.AppendBool(o, z.HasChildren)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Node) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zwht uint32
	zwht, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		return
	}
	for zwht > 0 {
		zwht--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "Path":
			z.Path, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		case "Leaf":
			z.Leaf, bts, err = msgp.ReadBoolBytes(bts)
			if err != nil {
				return
			}
		case "Defs":
			var zhct uint32
			zhct, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				return
			}
			if cap(z.Defs) >= int(zhct) {
				z.Defs = (z.Defs)[:zhct]
			} else {
				z.Defs = make([]Archive, zhct)
			}
			for zbai := range z.Defs {
				bts, err = z.Defs[zbai].UnmarshalMsg(bts)
				if err != nil {
					return
				}
			}
		case "HasChildren":
			z.HasChildren, bts, err = msgp.ReadBoolBytes(bts)
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
func (z *Node) Msgsize() (s int) {
	s = 1 + 5 + msgp.StringPrefixSize + len(z.Path) + 5 + msgp.BoolSize + 5 + msgp.ArrayHeaderSize
	for zbai := range z.Defs {
		s += z.Defs[zbai].Msgsize()
	}
	s += 12 + msgp.BoolSize
	return
}
