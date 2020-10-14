package models

import (
	"github.com/grafana/metrictank/schema"
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *Series) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "Target":
			z.Target, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "Target")
				return
			}
		case "Tags":
			var zb0002 uint32
			zb0002, err = dc.ReadMapHeader()
			if err != nil {
				err = msgp.WrapError(err, "Tags")
				return
			}
			if z.Tags == nil {
				z.Tags = make(map[string]string, zb0002)
			} else if len(z.Tags) > 0 {
				for key := range z.Tags {
					delete(z.Tags, key)
				}
			}
			for zb0002 > 0 {
				zb0002--
				var za0001 string
				var za0002 string
				za0001, err = dc.ReadString()
				if err != nil {
					err = msgp.WrapError(err, "Tags")
					return
				}
				za0002, err = dc.ReadString()
				if err != nil {
					err = msgp.WrapError(err, "Tags", za0001)
					return
				}
				z.Tags[za0001] = za0002
			}
		case "Interval":
			z.Interval, err = dc.ReadUint32()
			if err != nil {
				err = msgp.WrapError(err, "Interval")
				return
			}
		case "QueryPatt":
			z.QueryPatt, err = dc.ReadString()
			if err != nil {
				err = msgp.WrapError(err, "QueryPatt")
				return
			}
		case "QueryFrom":
			z.QueryFrom, err = dc.ReadUint32()
			if err != nil {
				err = msgp.WrapError(err, "QueryFrom")
				return
			}
		case "QueryTo":
			z.QueryTo, err = dc.ReadUint32()
			if err != nil {
				err = msgp.WrapError(err, "QueryTo")
				return
			}
		case "QueryCons":
			err = z.QueryCons.DecodeMsg(dc)
			if err != nil {
				err = msgp.WrapError(err, "QueryCons")
				return
			}
		case "Consolidator":
			err = z.Consolidator.DecodeMsg(dc)
			if err != nil {
				err = msgp.WrapError(err, "Consolidator")
				return
			}
		case "QueryMDP":
			z.QueryMDP, err = dc.ReadUint32()
			if err != nil {
				err = msgp.WrapError(err, "QueryMDP")
				return
			}
		case "QueryPNGroup":
			err = z.QueryPNGroup.DecodeMsg(dc)
			if err != nil {
				err = msgp.WrapError(err, "QueryPNGroup")
				return
			}
		case "Meta":
			var zb0003 uint32
			zb0003, err = dc.ReadArrayHeader()
			if err != nil {
				err = msgp.WrapError(err, "Meta")
				return
			}
			if cap(z.Meta) >= int(zb0003) {
				z.Meta = (z.Meta)[:zb0003]
			} else {
				z.Meta = make(SeriesMeta, zb0003)
			}
			for za0003 := range z.Meta {
				err = z.Meta[za0003].DecodeMsg(dc)
				if err != nil {
					err = msgp.WrapError(err, "Meta", za0003)
					return
				}
			}
		case "Datapoints":
			var zb0004 uint32
			zb0004, err = dc.ReadArrayHeader()
			if err != nil {
				err = msgp.WrapError(err, "Datapoints")
				return
			}
			if cap(z.Datapoints) >= int(zb0004) {
				z.Datapoints = (z.Datapoints)[:zb0004]
			} else {
				z.Datapoints = make([]schema.Point, zb0004)
			}
			for za0004 := range z.Datapoints {
				err = z.Datapoints[za0004].DecodeMsg(dc)
				if err != nil {
					err = msgp.WrapError(err, "Datapoints", za0004)
					return
				}
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
func (z *Series) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 12
	// write "Target"
	err = en.Append(0x8c, 0xa6, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74)
	if err != nil {
		return
	}
	err = en.WriteString(z.Target)
	if err != nil {
		err = msgp.WrapError(err, "Target")
		return
	}
	// write "Tags"
	err = en.Append(0xa4, 0x54, 0x61, 0x67, 0x73)
	if err != nil {
		return
	}
	err = en.WriteMapHeader(uint32(len(z.Tags)))
	if err != nil {
		err = msgp.WrapError(err, "Tags")
		return
	}
	for za0001, za0002 := range z.Tags {
		err = en.WriteString(za0001)
		if err != nil {
			err = msgp.WrapError(err, "Tags")
			return
		}
		err = en.WriteString(za0002)
		if err != nil {
			err = msgp.WrapError(err, "Tags", za0001)
			return
		}
	}
	// write "Interval"
	err = en.Append(0xa8, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x76, 0x61, 0x6c)
	if err != nil {
		return
	}
	err = en.WriteUint32(z.Interval)
	if err != nil {
		err = msgp.WrapError(err, "Interval")
		return
	}
	// write "QueryPatt"
	err = en.Append(0xa9, 0x51, 0x75, 0x65, 0x72, 0x79, 0x50, 0x61, 0x74, 0x74)
	if err != nil {
		return
	}
	err = en.WriteString(z.QueryPatt)
	if err != nil {
		err = msgp.WrapError(err, "QueryPatt")
		return
	}
	// write "QueryFrom"
	err = en.Append(0xa9, 0x51, 0x75, 0x65, 0x72, 0x79, 0x46, 0x72, 0x6f, 0x6d)
	if err != nil {
		return
	}
	err = en.WriteUint32(z.QueryFrom)
	if err != nil {
		err = msgp.WrapError(err, "QueryFrom")
		return
	}
	// write "QueryTo"
	err = en.Append(0xa7, 0x51, 0x75, 0x65, 0x72, 0x79, 0x54, 0x6f)
	if err != nil {
		return
	}
	err = en.WriteUint32(z.QueryTo)
	if err != nil {
		err = msgp.WrapError(err, "QueryTo")
		return
	}
	// write "QueryCons"
	err = en.Append(0xa9, 0x51, 0x75, 0x65, 0x72, 0x79, 0x43, 0x6f, 0x6e, 0x73)
	if err != nil {
		return
	}
	err = z.QueryCons.EncodeMsg(en)
	if err != nil {
		err = msgp.WrapError(err, "QueryCons")
		return
	}
	// write "Consolidator"
	err = en.Append(0xac, 0x43, 0x6f, 0x6e, 0x73, 0x6f, 0x6c, 0x69, 0x64, 0x61, 0x74, 0x6f, 0x72)
	if err != nil {
		return
	}
	err = z.Consolidator.EncodeMsg(en)
	if err != nil {
		err = msgp.WrapError(err, "Consolidator")
		return
	}
	// write "QueryMDP"
	err = en.Append(0xa8, 0x51, 0x75, 0x65, 0x72, 0x79, 0x4d, 0x44, 0x50)
	if err != nil {
		return
	}
	err = en.WriteUint32(z.QueryMDP)
	if err != nil {
		err = msgp.WrapError(err, "QueryMDP")
		return
	}
	// write "QueryPNGroup"
	err = en.Append(0xac, 0x51, 0x75, 0x65, 0x72, 0x79, 0x50, 0x4e, 0x47, 0x72, 0x6f, 0x75, 0x70)
	if err != nil {
		return
	}
	err = z.QueryPNGroup.EncodeMsg(en)
	if err != nil {
		err = msgp.WrapError(err, "QueryPNGroup")
		return
	}
	// write "Meta"
	err = en.Append(0xa4, 0x4d, 0x65, 0x74, 0x61)
	if err != nil {
		return
	}
	err = en.WriteArrayHeader(uint32(len(z.Meta)))
	if err != nil {
		err = msgp.WrapError(err, "Meta")
		return
	}
	for za0003 := range z.Meta {
		err = z.Meta[za0003].EncodeMsg(en)
		if err != nil {
			err = msgp.WrapError(err, "Meta", za0003)
			return
		}
	}
	// write "Datapoints"
	err = en.Append(0xaa, 0x44, 0x61, 0x74, 0x61, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x73)
	if err != nil {
		return
	}
	err = en.WriteArrayHeader(uint32(len(z.Datapoints)))
	if err != nil {
		err = msgp.WrapError(err, "Datapoints")
		return
	}
	for za0004 := range z.Datapoints {
		err = z.Datapoints[za0004].EncodeMsg(en)
		if err != nil {
			err = msgp.WrapError(err, "Datapoints", za0004)
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *Series) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 12
	// string "Target"
	o = append(o, 0x8c, 0xa6, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74)
	o = msgp.AppendString(o, z.Target)
	// string "Tags"
	o = append(o, 0xa4, 0x54, 0x61, 0x67, 0x73)
	o = msgp.AppendMapHeader(o, uint32(len(z.Tags)))
	for za0001, za0002 := range z.Tags {
		o = msgp.AppendString(o, za0001)
		o = msgp.AppendString(o, za0002)
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
		err = msgp.WrapError(err, "QueryCons")
		return
	}
	// string "Consolidator"
	o = append(o, 0xac, 0x43, 0x6f, 0x6e, 0x73, 0x6f, 0x6c, 0x69, 0x64, 0x61, 0x74, 0x6f, 0x72)
	o, err = z.Consolidator.MarshalMsg(o)
	if err != nil {
		err = msgp.WrapError(err, "Consolidator")
		return
	}
	// string "QueryMDP"
	o = append(o, 0xa8, 0x51, 0x75, 0x65, 0x72, 0x79, 0x4d, 0x44, 0x50)
	o = msgp.AppendUint32(o, z.QueryMDP)
	// string "QueryPNGroup"
	o = append(o, 0xac, 0x51, 0x75, 0x65, 0x72, 0x79, 0x50, 0x4e, 0x47, 0x72, 0x6f, 0x75, 0x70)
	o, err = z.QueryPNGroup.MarshalMsg(o)
	if err != nil {
		err = msgp.WrapError(err, "QueryPNGroup")
		return
	}
	// string "Meta"
	o = append(o, 0xa4, 0x4d, 0x65, 0x74, 0x61)
	o = msgp.AppendArrayHeader(o, uint32(len(z.Meta)))
	for za0003 := range z.Meta {
		o, err = z.Meta[za0003].MarshalMsg(o)
		if err != nil {
			err = msgp.WrapError(err, "Meta", za0003)
			return
		}
	}
	// string "Datapoints"
	o = append(o, 0xaa, 0x44, 0x61, 0x74, 0x61, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x73)
	o = msgp.AppendArrayHeader(o, uint32(len(z.Datapoints)))
	for za0004 := range z.Datapoints {
		o, err = z.Datapoints[za0004].MarshalMsg(o)
		if err != nil {
			err = msgp.WrapError(err, "Datapoints", za0004)
			return
		}
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Series) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "Target":
			z.Target, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Target")
				return
			}
		case "Tags":
			var zb0002 uint32
			zb0002, bts, err = msgp.ReadMapHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Tags")
				return
			}
			if z.Tags == nil {
				z.Tags = make(map[string]string, zb0002)
			} else if len(z.Tags) > 0 {
				for key := range z.Tags {
					delete(z.Tags, key)
				}
			}
			for zb0002 > 0 {
				var za0001 string
				var za0002 string
				zb0002--
				za0001, bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "Tags")
					return
				}
				za0002, bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					err = msgp.WrapError(err, "Tags", za0001)
					return
				}
				z.Tags[za0001] = za0002
			}
		case "Interval":
			z.Interval, bts, err = msgp.ReadUint32Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Interval")
				return
			}
		case "QueryPatt":
			z.QueryPatt, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "QueryPatt")
				return
			}
		case "QueryFrom":
			z.QueryFrom, bts, err = msgp.ReadUint32Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "QueryFrom")
				return
			}
		case "QueryTo":
			z.QueryTo, bts, err = msgp.ReadUint32Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "QueryTo")
				return
			}
		case "QueryCons":
			bts, err = z.QueryCons.UnmarshalMsg(bts)
			if err != nil {
				err = msgp.WrapError(err, "QueryCons")
				return
			}
		case "Consolidator":
			bts, err = z.Consolidator.UnmarshalMsg(bts)
			if err != nil {
				err = msgp.WrapError(err, "Consolidator")
				return
			}
		case "QueryMDP":
			z.QueryMDP, bts, err = msgp.ReadUint32Bytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "QueryMDP")
				return
			}
		case "QueryPNGroup":
			bts, err = z.QueryPNGroup.UnmarshalMsg(bts)
			if err != nil {
				err = msgp.WrapError(err, "QueryPNGroup")
				return
			}
		case "Meta":
			var zb0003 uint32
			zb0003, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Meta")
				return
			}
			if cap(z.Meta) >= int(zb0003) {
				z.Meta = (z.Meta)[:zb0003]
			} else {
				z.Meta = make(SeriesMeta, zb0003)
			}
			for za0003 := range z.Meta {
				bts, err = z.Meta[za0003].UnmarshalMsg(bts)
				if err != nil {
					err = msgp.WrapError(err, "Meta", za0003)
					return
				}
			}
		case "Datapoints":
			var zb0004 uint32
			zb0004, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				err = msgp.WrapError(err, "Datapoints")
				return
			}
			if cap(z.Datapoints) >= int(zb0004) {
				z.Datapoints = (z.Datapoints)[:zb0004]
			} else {
				z.Datapoints = make([]schema.Point, zb0004)
			}
			for za0004 := range z.Datapoints {
				bts, err = z.Datapoints[za0004].UnmarshalMsg(bts)
				if err != nil {
					err = msgp.WrapError(err, "Datapoints", za0004)
					return
				}
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
func (z *Series) Msgsize() (s int) {
	s = 1 + 7 + msgp.StringPrefixSize + len(z.Target) + 5 + msgp.MapHeaderSize
	if z.Tags != nil {
		for za0001, za0002 := range z.Tags {
			_ = za0002
			s += msgp.StringPrefixSize + len(za0001) + msgp.StringPrefixSize + len(za0002)
		}
	}
	s += 9 + msgp.Uint32Size + 10 + msgp.StringPrefixSize + len(z.QueryPatt) + 10 + msgp.Uint32Size + 8 + msgp.Uint32Size + 10 + z.QueryCons.Msgsize() + 13 + z.Consolidator.Msgsize() + 9 + msgp.Uint32Size + 13 + z.QueryPNGroup.Msgsize() + 5 + msgp.ArrayHeaderSize
	for za0003 := range z.Meta {
		s += z.Meta[za0003].Msgsize()
	}
	s += 11 + msgp.ArrayHeaderSize
	for za0004 := range z.Datapoints {
		s += z.Datapoints[za0004].Msgsize()
	}
	return
}
