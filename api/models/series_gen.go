package models

// NOTE: THIS FILE WAS PRODUCED BY THE
// MSGP CODE GENERATION TOOL (github.com/tinylib/msgp)
// DO NOT EDIT

import (
	"github.com/tinylib/msgp/msgp"
	"gopkg.in/raintank/schema.v1"
)

// DecodeMsg implements msgp.Decodable
func (z *Series) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "Target":
			z.Target, err = dc.ReadString()
			if err != nil {
				return
			}
		case "Datapoints":
			var zajw uint32
			zajw, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.Datapoints) >= int(zajw) {
				z.Datapoints = (z.Datapoints)[:zajw]
			} else {
				z.Datapoints = make([]schema.Point, zajw)
			}
			for zxvk := range z.Datapoints {
				err = z.Datapoints[zxvk].DecodeMsg(dc)
				if err != nil {
					return
				}
			}
		case "Tags":
			var zwht uint32
			zwht, err = dc.ReadMapHeader()
			if err != nil {
				return
			}
			if z.Tags == nil && zwht > 0 {
				z.Tags = make(map[string]string, zwht)
			} else if len(z.Tags) > 0 {
				for key, _ := range z.Tags {
					delete(z.Tags, key)
				}
			}
			for zwht > 0 {
				zwht--
				var zbzg string
				var zbai string
				zbzg, err = dc.ReadString()
				if err != nil {
					return
				}
				zbai, err = dc.ReadString()
				if err != nil {
					return
				}
				z.Tags[zbzg] = zbai
			}
		case "Interval":
			z.Interval, err = dc.ReadUint32()
			if err != nil {
				return
			}
		case "QueryPatt":
			z.QueryPatt, err = dc.ReadString()
			if err != nil {
				return
			}
		case "QueryFrom":
			z.QueryFrom, err = dc.ReadUint32()
			if err != nil {
				return
			}
		case "QueryTo":
			z.QueryTo, err = dc.ReadUint32()
			if err != nil {
				return
			}
		case "QueryCons":
			err = z.QueryCons.DecodeMsg(dc)
			if err != nil {
				return
			}
		case "Consolidator":
			err = z.Consolidator.DecodeMsg(dc)
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
func (z *Series) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 9
	// write "Target"
	err = en.Append(0x89, 0xa6, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Target)
	if err != nil {
		return
	}
	// write "Datapoints"
	err = en.Append(0xaa, 0x44, 0x61, 0x74, 0x61, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x73)
	if err != nil {
		return err
	}
	err = en.WriteArrayHeader(uint32(len(z.Datapoints)))
	if err != nil {
		return
	}
	for zxvk := range z.Datapoints {
		err = z.Datapoints[zxvk].EncodeMsg(en)
		if err != nil {
			return
		}
	}
	// write "Tags"
	err = en.Append(0xa4, 0x54, 0x61, 0x67, 0x73)
	if err != nil {
		return err
	}
	err = en.WriteMapHeader(uint32(len(z.Tags)))
	if err != nil {
		return
	}
	for zbzg, zbai := range z.Tags {
		err = en.WriteString(zbzg)
		if err != nil {
			return
		}
		err = en.WriteString(zbai)
		if err != nil {
			return
		}
	}
	// write "Interval"
	err = en.Append(0xa8, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x76, 0x61, 0x6c)
	if err != nil {
		return err
	}
	err = en.WriteUint32(z.Interval)
	if err != nil {
		return
	}
	// write "QueryPatt"
	err = en.Append(0xa9, 0x51, 0x75, 0x65, 0x72, 0x79, 0x50, 0x61, 0x74, 0x74)
	if err != nil {
		return err
	}
	err = en.WriteString(z.QueryPatt)
	if err != nil {
		return
	}
	// write "QueryFrom"
	err = en.Append(0xa9, 0x51, 0x75, 0x65, 0x72, 0x79, 0x46, 0x72, 0x6f, 0x6d)
	if err != nil {
		return err
	}
	err = en.WriteUint32(z.QueryFrom)
	if err != nil {
		return
	}
	// write "QueryTo"
	err = en.Append(0xa7, 0x51, 0x75, 0x65, 0x72, 0x79, 0x54, 0x6f)
	if err != nil {
		return err
	}
	err = en.WriteUint32(z.QueryTo)
	if err != nil {
		return
	}
	// write "QueryCons"
	err = en.Append(0xa9, 0x51, 0x75, 0x65, 0x72, 0x79, 0x43, 0x6f, 0x6e, 0x73)
	if err != nil {
		return err
	}
	err = z.QueryCons.EncodeMsg(en)
	if err != nil {
		return
	}
	// write "Consolidator"
	err = en.Append(0xac, 0x43, 0x6f, 0x6e, 0x73, 0x6f, 0x6c, 0x69, 0x64, 0x61, 0x74, 0x6f, 0x72)
	if err != nil {
		return err
	}
	err = z.Consolidator.EncodeMsg(en)
	if err != nil {
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *Series) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 9
	// string "Target"
	o = append(o, 0x89, 0xa6, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74)
	o = msgp.AppendString(o, z.Target)
	// string "Datapoints"
	o = append(o, 0xaa, 0x44, 0x61, 0x74, 0x61, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x73)
	o = msgp.AppendArrayHeader(o, uint32(len(z.Datapoints)))
	for zxvk := range z.Datapoints {
		o, err = z.Datapoints[zxvk].MarshalMsg(o)
		if err != nil {
			return
		}
	}
	// string "Tags"
	o = append(o, 0xa4, 0x54, 0x61, 0x67, 0x73)
	o = msgp.AppendMapHeader(o, uint32(len(z.Tags)))
	for zbzg, zbai := range z.Tags {
		o = msgp.AppendString(o, zbzg)
		o = msgp.AppendString(o, zbai)
	}
	// string "Interval"
	o = append(o, 0xa8, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x76, 0x61, 0x6c)
	o = msgp.AppendUint32(o, z.Interval)
	// string "QueryPatt"
	o = append(o, 0xa9, 0x51, 0x75, 0x65, 0x72, 0x79, 0x50, 0x61, 0x74, 0x74)
	o = msgp.AppendString(o, z.QueryPatt)
	// string "QueryFrom"
	o = append(o, 0xa9, 0x51, 0x75, 0x65, 0x72, 0x79, 0x46, 0x72, 0x6f, 0x6d)
	o = msgp.AppendUint32(o, z.QueryFrom)
	// string "QueryTo"
	o = append(o, 0xa7, 0x51, 0x75, 0x65, 0x72, 0x79, 0x54, 0x6f)
	o = msgp.AppendUint32(o, z.QueryTo)
	// string "QueryCons"
	o = append(o, 0xa9, 0x51, 0x75, 0x65, 0x72, 0x79, 0x43, 0x6f, 0x6e, 0x73)
	o, err = z.QueryCons.MarshalMsg(o)
	if err != nil {
		return
	}
	// string "Consolidator"
	o = append(o, 0xac, 0x43, 0x6f, 0x6e, 0x73, 0x6f, 0x6c, 0x69, 0x64, 0x61, 0x74, 0x6f, 0x72)
	o, err = z.Consolidator.MarshalMsg(o)
	if err != nil {
		return
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Series) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zhct uint32
	zhct, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		return
	}
	for zhct > 0 {
		zhct--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "Target":
			z.Target, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		case "Datapoints":
			var zcua uint32
			zcua, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				return
			}
			if cap(z.Datapoints) >= int(zcua) {
				z.Datapoints = (z.Datapoints)[:zcua]
			} else {
				z.Datapoints = make([]schema.Point, zcua)
			}
			for zxvk := range z.Datapoints {
				bts, err = z.Datapoints[zxvk].UnmarshalMsg(bts)
				if err != nil {
					return
				}
			}
		case "Tags":
			var zxhx uint32
			zxhx, bts, err = msgp.ReadMapHeaderBytes(bts)
			if err != nil {
				return
			}
			if z.Tags == nil && zxhx > 0 {
				z.Tags = make(map[string]string, zxhx)
			} else if len(z.Tags) > 0 {
				for key, _ := range z.Tags {
					delete(z.Tags, key)
				}
			}
			for zxhx > 0 {
				var zbzg string
				var zbai string
				zxhx--
				zbzg, bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					return
				}
				zbai, bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					return
				}
				z.Tags[zbzg] = zbai
			}
		case "Interval":
			z.Interval, bts, err = msgp.ReadUint32Bytes(bts)
			if err != nil {
				return
			}
		case "QueryPatt":
			z.QueryPatt, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		case "QueryFrom":
			z.QueryFrom, bts, err = msgp.ReadUint32Bytes(bts)
			if err != nil {
				return
			}
		case "QueryTo":
			z.QueryTo, bts, err = msgp.ReadUint32Bytes(bts)
			if err != nil {
				return
			}
		case "QueryCons":
			bts, err = z.QueryCons.UnmarshalMsg(bts)
			if err != nil {
				return
			}
		case "Consolidator":
			bts, err = z.Consolidator.UnmarshalMsg(bts)
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
func (z *Series) Msgsize() (s int) {
	s = 1 + 7 + msgp.StringPrefixSize + len(z.Target) + 11 + msgp.ArrayHeaderSize
	for zxvk := range z.Datapoints {
		s += z.Datapoints[zxvk].Msgsize()
	}
	s += 5 + msgp.MapHeaderSize
	if z.Tags != nil {
		for zbzg, zbai := range z.Tags {
			_ = zbai
			s += msgp.StringPrefixSize + len(zbzg) + msgp.StringPrefixSize + len(zbai)
		}
	}
	s += 9 + msgp.Uint32Size + 10 + msgp.StringPrefixSize + len(z.QueryPatt) + 10 + msgp.Uint32Size + 8 + msgp.Uint32Size + 10 + z.QueryCons.Msgsize() + 13 + z.Consolidator.Msgsize()
	return
}

// DecodeMsg implements msgp.Decodable
func (z *SeriesByTarget) DecodeMsg(dc *msgp.Reader) (err error) {
	var zpks uint32
	zpks, err = dc.ReadArrayHeader()
	if err != nil {
		return
	}
	if cap((*z)) >= int(zpks) {
		(*z) = (*z)[:zpks]
	} else {
		(*z) = make(SeriesByTarget, zpks)
	}
	for zdaf := range *z {
		err = (*z)[zdaf].DecodeMsg(dc)
		if err != nil {
			return
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z SeriesByTarget) EncodeMsg(en *msgp.Writer) (err error) {
	err = en.WriteArrayHeader(uint32(len(z)))
	if err != nil {
		return
	}
	for zjfb := range z {
		err = z[zjfb].EncodeMsg(en)
		if err != nil {
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z SeriesByTarget) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	o = msgp.AppendArrayHeader(o, uint32(len(z)))
	for zjfb := range z {
		o, err = z[zjfb].MarshalMsg(o)
		if err != nil {
			return
		}
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *SeriesByTarget) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var zeff uint32
	zeff, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		return
	}
	if cap((*z)) >= int(zeff) {
		(*z) = (*z)[:zeff]
	} else {
		(*z) = make(SeriesByTarget, zeff)
	}
	for zcxo := range *z {
		bts, err = (*z)[zcxo].UnmarshalMsg(bts)
		if err != nil {
			return
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z SeriesByTarget) Msgsize() (s int) {
	s = msgp.ArrayHeaderSize
	for zrsw := range z {
		s += z[zrsw].Msgsize()
	}
	return
}
