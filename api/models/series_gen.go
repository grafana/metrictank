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
		case "Target":
			z.Target, err = dc.ReadString()
			if err != nil {
				return
			}
		case "Datapoints":
			var zb0002 uint32
			zb0002, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.Datapoints) >= int(zb0002) {
				z.Datapoints = (z.Datapoints)[:zb0002]
			} else {
				z.Datapoints = make([]schema.Point, zb0002)
			}
			for za0001 := range z.Datapoints {
				err = z.Datapoints[za0001].DecodeMsg(dc)
				if err != nil {
					return
				}
			}
		case "Tags":
			var zb0003 uint32
			zb0003, err = dc.ReadMapHeader()
			if err != nil {
				return
			}
			if z.Tags == nil && zb0003 > 0 {
				z.Tags = make(map[string]string, zb0003)
			} else if len(z.Tags) > 0 {
				for key, _ := range z.Tags {
					delete(z.Tags, key)
				}
			}
			for zb0003 > 0 {
				zb0003--
				var za0002 string
				var za0003 string
				za0002, err = dc.ReadString()
				if err != nil {
					return
				}
				za0003, err = dc.ReadString()
				if err != nil {
					return
				}
				z.Tags[za0002] = za0003
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
		return
	}
	err = en.WriteString(z.Target)
	if err != nil {
		return
	}
	// write "Datapoints"
	err = en.Append(0xaa, 0x44, 0x61, 0x74, 0x61, 0x70, 0x6f, 0x69, 0x6e, 0x74, 0x73)
	if err != nil {
		return
	}
	err = en.WriteArrayHeader(uint32(len(z.Datapoints)))
	if err != nil {
		return
	}
	for za0001 := range z.Datapoints {
		err = z.Datapoints[za0001].EncodeMsg(en)
		if err != nil {
			return
		}
	}
	// write "Tags"
	err = en.Append(0xa4, 0x54, 0x61, 0x67, 0x73)
	if err != nil {
		return
	}
	err = en.WriteMapHeader(uint32(len(z.Tags)))
	if err != nil {
		return
	}
	for za0002, za0003 := range z.Tags {
		err = en.WriteString(za0002)
		if err != nil {
			return
		}
		err = en.WriteString(za0003)
		if err != nil {
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
		return
	}
	// write "QueryPatt"
	err = en.Append(0xa9, 0x51, 0x75, 0x65, 0x72, 0x79, 0x50, 0x61, 0x74, 0x74)
	if err != nil {
		return
	}
	err = en.WriteString(z.QueryPatt)
	if err != nil {
		return
	}
	// write "QueryFrom"
	err = en.Append(0xa9, 0x51, 0x75, 0x65, 0x72, 0x79, 0x46, 0x72, 0x6f, 0x6d)
	if err != nil {
		return
	}
	err = en.WriteUint32(z.QueryFrom)
	if err != nil {
		return
	}
	// write "QueryTo"
	err = en.Append(0xa7, 0x51, 0x75, 0x65, 0x72, 0x79, 0x54, 0x6f)
	if err != nil {
		return
	}
	err = en.WriteUint32(z.QueryTo)
	if err != nil {
		return
	}
	// write "QueryCons"
	err = en.Append(0xa9, 0x51, 0x75, 0x65, 0x72, 0x79, 0x43, 0x6f, 0x6e, 0x73)
	if err != nil {
		return
	}
	err = z.QueryCons.EncodeMsg(en)
	if err != nil {
		return
	}
	// write "Consolidator"
	err = en.Append(0xac, 0x43, 0x6f, 0x6e, 0x73, 0x6f, 0x6c, 0x69, 0x64, 0x61, 0x74, 0x6f, 0x72)
	if err != nil {
		return
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
	for za0001 := range z.Datapoints {
		o, err = z.Datapoints[za0001].MarshalMsg(o)
		if err != nil {
			return
		}
	}
	// string "Tags"
	o = append(o, 0xa4, 0x54, 0x61, 0x67, 0x73)
	o = msgp.AppendMapHeader(o, uint32(len(z.Tags)))
	for za0002, za0003 := range z.Tags {
		o = msgp.AppendString(o, za0002)
		o = msgp.AppendString(o, za0003)
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
		case "Target":
			z.Target, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		case "Datapoints":
			var zb0002 uint32
			zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				return
			}
			if cap(z.Datapoints) >= int(zb0002) {
				z.Datapoints = (z.Datapoints)[:zb0002]
			} else {
				z.Datapoints = make([]schema.Point, zb0002)
			}
			for za0001 := range z.Datapoints {
				bts, err = z.Datapoints[za0001].UnmarshalMsg(bts)
				if err != nil {
					return
				}
			}
		case "Tags":
			var zb0003 uint32
			zb0003, bts, err = msgp.ReadMapHeaderBytes(bts)
			if err != nil {
				return
			}
			if z.Tags == nil && zb0003 > 0 {
				z.Tags = make(map[string]string, zb0003)
			} else if len(z.Tags) > 0 {
				for key, _ := range z.Tags {
					delete(z.Tags, key)
				}
			}
			for zb0003 > 0 {
				var za0002 string
				var za0003 string
				zb0003--
				za0002, bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					return
				}
				za0003, bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					return
				}
				z.Tags[za0002] = za0003
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
	for za0001 := range z.Datapoints {
		s += z.Datapoints[za0001].Msgsize()
	}
	s += 5 + msgp.MapHeaderSize
	if z.Tags != nil {
		for za0002, za0003 := range z.Tags {
			_ = za0003
			s += msgp.StringPrefixSize + len(za0002) + msgp.StringPrefixSize + len(za0003)
		}
	}
	s += 9 + msgp.Uint32Size + 10 + msgp.StringPrefixSize + len(z.QueryPatt) + 10 + msgp.Uint32Size + 8 + msgp.Uint32Size + 10 + z.QueryCons.Msgsize() + 13 + z.Consolidator.Msgsize()
	return
}

// DecodeMsg implements msgp.Decodable
func (z *SeriesByTarget) DecodeMsg(dc *msgp.Reader) (err error) {
	var zb0002 uint32
	zb0002, err = dc.ReadArrayHeader()
	if err != nil {
		return
	}
	if cap((*z)) >= int(zb0002) {
		(*z) = (*z)[:zb0002]
	} else {
		(*z) = make(SeriesByTarget, zb0002)
	}
	for zb0001 := range *z {
		err = (*z)[zb0001].DecodeMsg(dc)
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
	for zb0003 := range z {
		err = z[zb0003].EncodeMsg(en)
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
	for zb0003 := range z {
		o, err = z[zb0003].MarshalMsg(o)
		if err != nil {
			return
		}
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *SeriesByTarget) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var zb0002 uint32
	zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		return
	}
	if cap((*z)) >= int(zb0002) {
		(*z) = (*z)[:zb0002]
	} else {
		(*z) = make(SeriesByTarget, zb0002)
	}
	for zb0001 := range *z {
		bts, err = (*z)[zb0001].UnmarshalMsg(bts)
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
	for zb0003 := range z {
		s += z[zb0003].Msgsize()
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *SeriesForPickle) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "name":
			z.Name, err = dc.ReadString()
			if err != nil {
				return
			}
		case "start":
			z.Start, err = dc.ReadUint32()
			if err != nil {
				return
			}
		case "end":
			z.End, err = dc.ReadUint32()
			if err != nil {
				return
			}
		case "step":
			z.Step, err = dc.ReadUint32()
			if err != nil {
				return
			}
		case "values":
			var zb0002 uint32
			zb0002, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.Values) >= int(zb0002) {
				z.Values = (z.Values)[:zb0002]
			} else {
				z.Values = make([]interface{}, zb0002)
			}
			for za0001 := range z.Values {
				z.Values[za0001], err = dc.ReadIntf()
				if err != nil {
					return
				}
			}
		case "pathExpression":
			z.PathExpression, err = dc.ReadString()
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
func (z *SeriesForPickle) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 6
	// write "name"
	err = en.Append(0x86, 0xa4, 0x6e, 0x61, 0x6d, 0x65)
	if err != nil {
		return
	}
	err = en.WriteString(z.Name)
	if err != nil {
		return
	}
	// write "start"
	err = en.Append(0xa5, 0x73, 0x74, 0x61, 0x72, 0x74)
	if err != nil {
		return
	}
	err = en.WriteUint32(z.Start)
	if err != nil {
		return
	}
	// write "end"
	err = en.Append(0xa3, 0x65, 0x6e, 0x64)
	if err != nil {
		return
	}
	err = en.WriteUint32(z.End)
	if err != nil {
		return
	}
	// write "step"
	err = en.Append(0xa4, 0x73, 0x74, 0x65, 0x70)
	if err != nil {
		return
	}
	err = en.WriteUint32(z.Step)
	if err != nil {
		return
	}
	// write "values"
	err = en.Append(0xa6, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73)
	if err != nil {
		return
	}
	err = en.WriteArrayHeader(uint32(len(z.Values)))
	if err != nil {
		return
	}
	for za0001 := range z.Values {
		err = en.WriteIntf(z.Values[za0001])
		if err != nil {
			return
		}
	}
	// write "pathExpression"
	err = en.Append(0xae, 0x70, 0x61, 0x74, 0x68, 0x45, 0x78, 0x70, 0x72, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e)
	if err != nil {
		return
	}
	err = en.WriteString(z.PathExpression)
	if err != nil {
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *SeriesForPickle) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 6
	// string "name"
	o = append(o, 0x86, 0xa4, 0x6e, 0x61, 0x6d, 0x65)
	o = msgp.AppendString(o, z.Name)
	// string "start"
	o = append(o, 0xa5, 0x73, 0x74, 0x61, 0x72, 0x74)
	o = msgp.AppendUint32(o, z.Start)
	// string "end"
	o = append(o, 0xa3, 0x65, 0x6e, 0x64)
	o = msgp.AppendUint32(o, z.End)
	// string "step"
	o = append(o, 0xa4, 0x73, 0x74, 0x65, 0x70)
	o = msgp.AppendUint32(o, z.Step)
	// string "values"
	o = append(o, 0xa6, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73)
	o = msgp.AppendArrayHeader(o, uint32(len(z.Values)))
	for za0001 := range z.Values {
		o, err = msgp.AppendIntf(o, z.Values[za0001])
		if err != nil {
			return
		}
	}
	// string "pathExpression"
	o = append(o, 0xae, 0x70, 0x61, 0x74, 0x68, 0x45, 0x78, 0x70, 0x72, 0x65, 0x73, 0x73, 0x69, 0x6f, 0x6e)
	o = msgp.AppendString(o, z.PathExpression)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *SeriesForPickle) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "name":
			z.Name, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		case "start":
			z.Start, bts, err = msgp.ReadUint32Bytes(bts)
			if err != nil {
				return
			}
		case "end":
			z.End, bts, err = msgp.ReadUint32Bytes(bts)
			if err != nil {
				return
			}
		case "step":
			z.Step, bts, err = msgp.ReadUint32Bytes(bts)
			if err != nil {
				return
			}
		case "values":
			var zb0002 uint32
			zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				return
			}
			if cap(z.Values) >= int(zb0002) {
				z.Values = (z.Values)[:zb0002]
			} else {
				z.Values = make([]interface{}, zb0002)
			}
			for za0001 := range z.Values {
				z.Values[za0001], bts, err = msgp.ReadIntfBytes(bts)
				if err != nil {
					return
				}
			}
		case "pathExpression":
			z.PathExpression, bts, err = msgp.ReadStringBytes(bts)
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
func (z *SeriesForPickle) Msgsize() (s int) {
	s = 1 + 5 + msgp.StringPrefixSize + len(z.Name) + 6 + msgp.Uint32Size + 4 + msgp.Uint32Size + 5 + msgp.Uint32Size + 7 + msgp.ArrayHeaderSize
	for za0001 := range z.Values {
		s += msgp.GuessSize(z.Values[za0001])
	}
	s += 15 + msgp.StringPrefixSize + len(z.PathExpression)
	return
}

// DecodeMsg implements msgp.Decodable
func (z *SeriesListForPickle) DecodeMsg(dc *msgp.Reader) (err error) {
	var zb0002 uint32
	zb0002, err = dc.ReadArrayHeader()
	if err != nil {
		return
	}
	if cap((*z)) >= int(zb0002) {
		(*z) = (*z)[:zb0002]
	} else {
		(*z) = make(SeriesListForPickle, zb0002)
	}
	for zb0001 := range *z {
		err = (*z)[zb0001].DecodeMsg(dc)
		if err != nil {
			return
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z SeriesListForPickle) EncodeMsg(en *msgp.Writer) (err error) {
	err = en.WriteArrayHeader(uint32(len(z)))
	if err != nil {
		return
	}
	for zb0003 := range z {
		err = z[zb0003].EncodeMsg(en)
		if err != nil {
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z SeriesListForPickle) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	o = msgp.AppendArrayHeader(o, uint32(len(z)))
	for zb0003 := range z {
		o, err = z[zb0003].MarshalMsg(o)
		if err != nil {
			return
		}
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *SeriesListForPickle) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var zb0002 uint32
	zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		return
	}
	if cap((*z)) >= int(zb0002) {
		(*z) = (*z)[:zb0002]
	} else {
		(*z) = make(SeriesListForPickle, zb0002)
	}
	for zb0001 := range *z {
		bts, err = (*z)[zb0001].UnmarshalMsg(bts)
		if err != nil {
			return
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z SeriesListForPickle) Msgsize() (s int) {
	s = msgp.ArrayHeaderSize
	for zb0003 := range z {
		s += z[zb0003].Msgsize()
	}
	return
}
