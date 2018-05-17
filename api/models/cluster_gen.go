package models

// NOTE: THIS FILE WAS PRODUCED BY THE
// MSGP CODE GENERATION TOOL (github.com/tinylib/msgp)
// DO NOT EDIT

import (
	"github.com/grafana/metrictank/idx"
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *GetDataResp) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "Series":
			var zb0002 uint32
			zb0002, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.Series) >= int(zb0002) {
				z.Series = (z.Series)[:zb0002]
			} else {
				z.Series = make([]Series, zb0002)
			}
			for za0001 := range z.Series {
				err = z.Series[za0001].DecodeMsg(dc)
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
func (z *GetDataResp) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 1
	// write "Series"
	err = en.Append(0x81, 0xa6, 0x53, 0x65, 0x72, 0x69, 0x65, 0x73)
	if err != nil {
		return
	}
	err = en.WriteArrayHeader(uint32(len(z.Series)))
	if err != nil {
		return
	}
	for za0001 := range z.Series {
		err = z.Series[za0001].EncodeMsg(en)
		if err != nil {
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *GetDataResp) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 1
	// string "Series"
	o = append(o, 0x81, 0xa6, 0x53, 0x65, 0x72, 0x69, 0x65, 0x73)
	o = msgp.AppendArrayHeader(o, uint32(len(z.Series)))
	for za0001 := range z.Series {
		o, err = z.Series[za0001].MarshalMsg(o)
		if err != nil {
			return
		}
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *GetDataResp) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "Series":
			var zb0002 uint32
			zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				return
			}
			if cap(z.Series) >= int(zb0002) {
				z.Series = (z.Series)[:zb0002]
			} else {
				z.Series = make([]Series, zb0002)
			}
			for za0001 := range z.Series {
				bts, err = z.Series[za0001].UnmarshalMsg(bts)
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

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *GetDataResp) Msgsize() (s int) {
	s = 1 + 7 + msgp.ArrayHeaderSize
	for za0001 := range z.Series {
		s += z.Series[za0001].Msgsize()
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *IndexFindByTagResp) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "Metrics":
			var zb0002 uint32
			zb0002, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.Metrics) >= int(zb0002) {
				z.Metrics = (z.Metrics)[:zb0002]
			} else {
				z.Metrics = make([]idx.Node, zb0002)
			}
			for za0001 := range z.Metrics {
				err = z.Metrics[za0001].DecodeMsg(dc)
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
func (z *IndexFindByTagResp) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 1
	// write "Metrics"
	err = en.Append(0x81, 0xa7, 0x4d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x73)
	if err != nil {
		return
	}
	err = en.WriteArrayHeader(uint32(len(z.Metrics)))
	if err != nil {
		return
	}
	for za0001 := range z.Metrics {
		err = z.Metrics[za0001].EncodeMsg(en)
		if err != nil {
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *IndexFindByTagResp) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 1
	// string "Metrics"
	o = append(o, 0x81, 0xa7, 0x4d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x73)
	o = msgp.AppendArrayHeader(o, uint32(len(z.Metrics)))
	for za0001 := range z.Metrics {
		o, err = z.Metrics[za0001].MarshalMsg(o)
		if err != nil {
			return
		}
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *IndexFindByTagResp) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "Metrics":
			var zb0002 uint32
			zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				return
			}
			if cap(z.Metrics) >= int(zb0002) {
				z.Metrics = (z.Metrics)[:zb0002]
			} else {
				z.Metrics = make([]idx.Node, zb0002)
			}
			for za0001 := range z.Metrics {
				bts, err = z.Metrics[za0001].UnmarshalMsg(bts)
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

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *IndexFindByTagResp) Msgsize() (s int) {
	s = 1 + 8 + msgp.ArrayHeaderSize
	for za0001 := range z.Metrics {
		s += z.Metrics[za0001].Msgsize()
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *IndexFindResp) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "Nodes":
			var zb0002 uint32
			zb0002, err = dc.ReadMapHeader()
			if err != nil {
				return
			}
			if z.Nodes == nil && zb0002 > 0 {
				z.Nodes = make(map[string][]idx.Node, zb0002)
			} else if len(z.Nodes) > 0 {
				for key := range z.Nodes {
					delete(z.Nodes, key)
				}
			}
			for zb0002 > 0 {
				zb0002--
				var za0001 string
				var za0002 []idx.Node
				za0001, err = dc.ReadString()
				if err != nil {
					return
				}
				var zb0003 uint32
				zb0003, err = dc.ReadArrayHeader()
				if err != nil {
					return
				}
				if cap(za0002) >= int(zb0003) {
					za0002 = (za0002)[:zb0003]
				} else {
					za0002 = make([]idx.Node, zb0003)
				}
				for za0003 := range za0002 {
					err = za0002[za0003].DecodeMsg(dc)
					if err != nil {
						return
					}
				}
				z.Nodes[za0001] = za0002
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
func (z *IndexFindResp) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 1
	// write "Nodes"
	err = en.Append(0x81, 0xa5, 0x4e, 0x6f, 0x64, 0x65, 0x73)
	if err != nil {
		return
	}
	err = en.WriteMapHeader(uint32(len(z.Nodes)))
	if err != nil {
		return
	}
	for za0001, za0002 := range z.Nodes {
		err = en.WriteString(za0001)
		if err != nil {
			return
		}
		err = en.WriteArrayHeader(uint32(len(za0002)))
		if err != nil {
			return
		}
		for za0003 := range za0002 {
			err = za0002[za0003].EncodeMsg(en)
			if err != nil {
				return
			}
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *IndexFindResp) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 1
	// string "Nodes"
	o = append(o, 0x81, 0xa5, 0x4e, 0x6f, 0x64, 0x65, 0x73)
	o = msgp.AppendMapHeader(o, uint32(len(z.Nodes)))
	for za0001, za0002 := range z.Nodes {
		o = msgp.AppendString(o, za0001)
		o = msgp.AppendArrayHeader(o, uint32(len(za0002)))
		for za0003 := range za0002 {
			o, err = za0002[za0003].MarshalMsg(o)
			if err != nil {
				return
			}
		}
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *IndexFindResp) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "Nodes":
			var zb0002 uint32
			zb0002, bts, err = msgp.ReadMapHeaderBytes(bts)
			if err != nil {
				return
			}
			if z.Nodes == nil && zb0002 > 0 {
				z.Nodes = make(map[string][]idx.Node, zb0002)
			} else if len(z.Nodes) > 0 {
				for key := range z.Nodes {
					delete(z.Nodes, key)
				}
			}
			for zb0002 > 0 {
				var za0001 string
				var za0002 []idx.Node
				zb0002--
				za0001, bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					return
				}
				var zb0003 uint32
				zb0003, bts, err = msgp.ReadArrayHeaderBytes(bts)
				if err != nil {
					return
				}
				if cap(za0002) >= int(zb0003) {
					za0002 = (za0002)[:zb0003]
				} else {
					za0002 = make([]idx.Node, zb0003)
				}
				for za0003 := range za0002 {
					bts, err = za0002[za0003].UnmarshalMsg(bts)
					if err != nil {
						return
					}
				}
				z.Nodes[za0001] = za0002
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
func (z *IndexFindResp) Msgsize() (s int) {
	s = 1 + 6 + msgp.MapHeaderSize
	if z.Nodes != nil {
		for za0001, za0002 := range z.Nodes {
			_ = za0002
			s += msgp.StringPrefixSize + len(za0001) + msgp.ArrayHeaderSize
			for za0003 := range za0002 {
				s += za0002[za0003].Msgsize()
			}
		}
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *IndexTagDelSeriesResp) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "Count":
			z.Count, err = dc.ReadInt()
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
func (z IndexTagDelSeriesResp) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 1
	// write "Count"
	err = en.Append(0x81, 0xa5, 0x43, 0x6f, 0x75, 0x6e, 0x74)
	if err != nil {
		return
	}
	err = en.WriteInt(z.Count)
	if err != nil {
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z IndexTagDelSeriesResp) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 1
	// string "Count"
	o = append(o, 0x81, 0xa5, 0x43, 0x6f, 0x75, 0x6e, 0x74)
	o = msgp.AppendInt(o, z.Count)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *IndexTagDelSeriesResp) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "Count":
			z.Count, bts, err = msgp.ReadIntBytes(bts)
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
func (z IndexTagDelSeriesResp) Msgsize() (s int) {
	s = 1 + 6 + msgp.IntSize
	return
}

// DecodeMsg implements msgp.Decodable
func (z *IndexTagDetailsResp) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "Values":
			var zb0002 uint32
			zb0002, err = dc.ReadMapHeader()
			if err != nil {
				return
			}
			if z.Values == nil && zb0002 > 0 {
				z.Values = make(map[string]uint64, zb0002)
			} else if len(z.Values) > 0 {
				for key := range z.Values {
					delete(z.Values, key)
				}
			}
			for zb0002 > 0 {
				zb0002--
				var za0001 string
				var za0002 uint64
				za0001, err = dc.ReadString()
				if err != nil {
					return
				}
				za0002, err = dc.ReadUint64()
				if err != nil {
					return
				}
				z.Values[za0001] = za0002
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
func (z *IndexTagDetailsResp) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 1
	// write "Values"
	err = en.Append(0x81, 0xa6, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x73)
	if err != nil {
		return
	}
	err = en.WriteMapHeader(uint32(len(z.Values)))
	if err != nil {
		return
	}
	for za0001, za0002 := range z.Values {
		err = en.WriteString(za0001)
		if err != nil {
			return
		}
		err = en.WriteUint64(za0002)
		if err != nil {
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *IndexTagDetailsResp) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 1
	// string "Values"
	o = append(o, 0x81, 0xa6, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x73)
	o = msgp.AppendMapHeader(o, uint32(len(z.Values)))
	for za0001, za0002 := range z.Values {
		o = msgp.AppendString(o, za0001)
		o = msgp.AppendUint64(o, za0002)
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *IndexTagDetailsResp) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "Values":
			var zb0002 uint32
			zb0002, bts, err = msgp.ReadMapHeaderBytes(bts)
			if err != nil {
				return
			}
			if z.Values == nil && zb0002 > 0 {
				z.Values = make(map[string]uint64, zb0002)
			} else if len(z.Values) > 0 {
				for key := range z.Values {
					delete(z.Values, key)
				}
			}
			for zb0002 > 0 {
				var za0001 string
				var za0002 uint64
				zb0002--
				za0001, bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					return
				}
				za0002, bts, err = msgp.ReadUint64Bytes(bts)
				if err != nil {
					return
				}
				z.Values[za0001] = za0002
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
func (z *IndexTagDetailsResp) Msgsize() (s int) {
	s = 1 + 7 + msgp.MapHeaderSize
	if z.Values != nil {
		for za0001, za0002 := range z.Values {
			_ = za0002
			s += msgp.StringPrefixSize + len(za0001) + msgp.Uint64Size
		}
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *IndexTagsResp) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "Tags":
			var zb0002 uint32
			zb0002, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.Tags) >= int(zb0002) {
				z.Tags = (z.Tags)[:zb0002]
			} else {
				z.Tags = make([]string, zb0002)
			}
			for za0001 := range z.Tags {
				z.Tags[za0001], err = dc.ReadString()
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
func (z *IndexTagsResp) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 1
	// write "Tags"
	err = en.Append(0x81, 0xa4, 0x54, 0x61, 0x67, 0x73)
	if err != nil {
		return
	}
	err = en.WriteArrayHeader(uint32(len(z.Tags)))
	if err != nil {
		return
	}
	for za0001 := range z.Tags {
		err = en.WriteString(z.Tags[za0001])
		if err != nil {
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *IndexTagsResp) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 1
	// string "Tags"
	o = append(o, 0x81, 0xa4, 0x54, 0x61, 0x67, 0x73)
	o = msgp.AppendArrayHeader(o, uint32(len(z.Tags)))
	for za0001 := range z.Tags {
		o = msgp.AppendString(o, z.Tags[za0001])
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *IndexTagsResp) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "Tags":
			var zb0002 uint32
			zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				return
			}
			if cap(z.Tags) >= int(zb0002) {
				z.Tags = (z.Tags)[:zb0002]
			} else {
				z.Tags = make([]string, zb0002)
			}
			for za0001 := range z.Tags {
				z.Tags[za0001], bts, err = msgp.ReadStringBytes(bts)
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

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z *IndexTagsResp) Msgsize() (s int) {
	s = 1 + 5 + msgp.ArrayHeaderSize
	for za0001 := range z.Tags {
		s += msgp.StringPrefixSize + len(z.Tags[za0001])
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *MetricsDeleteResp) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "DeletedDefs":
			z.DeletedDefs, err = dc.ReadInt()
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
func (z MetricsDeleteResp) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 1
	// write "DeletedDefs"
	err = en.Append(0x81, 0xab, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x64, 0x44, 0x65, 0x66, 0x73)
	if err != nil {
		return
	}
	err = en.WriteInt(z.DeletedDefs)
	if err != nil {
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z MetricsDeleteResp) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 1
	// string "DeletedDefs"
	o = append(o, 0x81, 0xab, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x64, 0x44, 0x65, 0x66, 0x73)
	o = msgp.AppendInt(o, z.DeletedDefs)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *MetricsDeleteResp) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "DeletedDefs":
			z.DeletedDefs, bts, err = msgp.ReadIntBytes(bts)
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
func (z MetricsDeleteResp) Msgsize() (s int) {
	s = 1 + 12 + msgp.IntSize
	return
}

// DecodeMsg implements msgp.Decodable
func (z *StringList) DecodeMsg(dc *msgp.Reader) (err error) {
	var zb0002 uint32
	zb0002, err = dc.ReadArrayHeader()
	if err != nil {
		return
	}
	if cap((*z)) >= int(zb0002) {
		(*z) = (*z)[:zb0002]
	} else {
		(*z) = make(StringList, zb0002)
	}
	for zb0001 := range *z {
		(*z)[zb0001], err = dc.ReadString()
		if err != nil {
			return
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z StringList) EncodeMsg(en *msgp.Writer) (err error) {
	err = en.WriteArrayHeader(uint32(len(z)))
	if err != nil {
		return
	}
	for zb0003 := range z {
		err = en.WriteString(z[zb0003])
		if err != nil {
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z StringList) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	o = msgp.AppendArrayHeader(o, uint32(len(z)))
	for zb0003 := range z {
		o = msgp.AppendString(o, z[zb0003])
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *StringList) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var zb0002 uint32
	zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		return
	}
	if cap((*z)) >= int(zb0002) {
		(*z) = (*z)[:zb0002]
	} else {
		(*z) = make(StringList, zb0002)
	}
	for zb0001 := range *z {
		(*z)[zb0001], bts, err = msgp.ReadStringBytes(bts)
		if err != nil {
			return
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z StringList) Msgsize() (s int) {
	s = msgp.ArrayHeaderSize
	for zb0003 := range z {
		s += msgp.StringPrefixSize + len(z[zb0003])
	}
	return
}
