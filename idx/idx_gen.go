package idx

// NOTE: THIS FILE WAS PRODUCED BY THE
// MSGP CODE GENERATION TOOL (github.com/tinylib/msgp)
// DO NOT EDIT

import (
	"github.com/tinylib/msgp/msgp"
	"gopkg.in/raintank/schema.v1"
)

// DecodeMsg implements msgp.Decodable
func (z *Node) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var isz uint32
	isz, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for isz > 0 {
		isz--
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
			var xsz uint32
			xsz, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.Defs) >= int(xsz) {
				z.Defs = z.Defs[:xsz]
			} else {
				z.Defs = make([]schema.MetricDefinition, xsz)
			}
			for xvk := range z.Defs {
				err = z.Defs[xvk].DecodeMsg(dc)
				if err != nil {
					return
				}
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
	// map header, size 3
	// write "Path"
	err = en.Append(0x83, 0xa4, 0x50, 0x61, 0x74, 0x68)
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
	for xvk := range z.Defs {
		err = z.Defs[xvk].EncodeMsg(en)
		if err != nil {
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *Node) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 3
	// string "Path"
	o = append(o, 0x83, 0xa4, 0x50, 0x61, 0x74, 0x68)
	o = msgp.AppendString(o, z.Path)
	// string "Leaf"
	o = append(o, 0xa4, 0x4c, 0x65, 0x61, 0x66)
	o = msgp.AppendBool(o, z.Leaf)
	// string "Defs"
	o = append(o, 0xa4, 0x44, 0x65, 0x66, 0x73)
	o = msgp.AppendArrayHeader(o, uint32(len(z.Defs)))
	for xvk := range z.Defs {
		o, err = z.Defs[xvk].MarshalMsg(o)
		if err != nil {
			return
		}
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Node) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var isz uint32
	isz, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		return
	}
	for isz > 0 {
		isz--
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
			var xsz uint32
			xsz, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				return
			}
			if cap(z.Defs) >= int(xsz) {
				z.Defs = z.Defs[:xsz]
			} else {
				z.Defs = make([]schema.MetricDefinition, xsz)
			}
			for xvk := range z.Defs {
				bts, err = z.Defs[xvk].UnmarshalMsg(bts)
				if err != nil {
					return
				}
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

func (z *Node) Msgsize() (s int) {
	s = 1 + 5 + msgp.StringPrefixSize + len(z.Path) + 5 + msgp.BoolSize + 5 + msgp.ArrayHeaderSize
	for xvk := range z.Defs {
		s += z.Defs[xvk].Msgsize()
	}
	return
}
