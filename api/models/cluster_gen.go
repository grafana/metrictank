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
	var zbzg uint32
	zbzg, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zbzg > 0 {
		zbzg--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "Series":
			var zbai uint32
			zbai, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.Series) >= int(zbai) {
				z.Series = (z.Series)[:zbai]
			} else {
				z.Series = make([]Series, zbai)
			}
			for zxvk := range z.Series {
				err = z.Series[zxvk].DecodeMsg(dc)
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
		return err
	}
	err = en.WriteArrayHeader(uint32(len(z.Series)))
	if err != nil {
		return
	}
	for zxvk := range z.Series {
		err = z.Series[zxvk].EncodeMsg(en)
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
	for zxvk := range z.Series {
		o, err = z.Series[zxvk].MarshalMsg(o)
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
	var zcmr uint32
	zcmr, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		return
	}
	for zcmr > 0 {
		zcmr--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "Series":
			var zajw uint32
			zajw, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				return
			}
			if cap(z.Series) >= int(zajw) {
				z.Series = (z.Series)[:zajw]
			} else {
				z.Series = make([]Series, zajw)
			}
			for zxvk := range z.Series {
				bts, err = z.Series[zxvk].UnmarshalMsg(bts)
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
	for zxvk := range z.Series {
		s += z.Series[zxvk].Msgsize()
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *IndexFindByTagResp) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zhct uint32
	zhct, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zhct > 0 {
		zhct--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "Metrics":
			var zcua uint32
			zcua, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.Metrics) >= int(zcua) {
				z.Metrics = (z.Metrics)[:zcua]
			} else {
				z.Metrics = make([]idx.Node, zcua)
			}
			for zwht := range z.Metrics {
				err = z.Metrics[zwht].DecodeMsg(dc)
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
		return err
	}
	err = en.WriteArrayHeader(uint32(len(z.Metrics)))
	if err != nil {
		return
	}
	for zwht := range z.Metrics {
		err = z.Metrics[zwht].EncodeMsg(en)
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
	for zwht := range z.Metrics {
		o, err = z.Metrics[zwht].MarshalMsg(o)
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
	var zxhx uint32
	zxhx, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		return
	}
	for zxhx > 0 {
		zxhx--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "Metrics":
			var zlqf uint32
			zlqf, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				return
			}
			if cap(z.Metrics) >= int(zlqf) {
				z.Metrics = (z.Metrics)[:zlqf]
			} else {
				z.Metrics = make([]idx.Node, zlqf)
			}
			for zwht := range z.Metrics {
				bts, err = z.Metrics[zwht].UnmarshalMsg(bts)
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
	for zwht := range z.Metrics {
		s += z.Metrics[zwht].Msgsize()
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *IndexFindResp) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zcxo uint32
	zcxo, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zcxo > 0 {
		zcxo--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "Nodes":
			var zeff uint32
			zeff, err = dc.ReadMapHeader()
			if err != nil {
				return
			}
			if z.Nodes == nil && zeff > 0 {
				z.Nodes = make(map[string][]idx.Node, zeff)
			} else if len(z.Nodes) > 0 {
				for key, _ := range z.Nodes {
					delete(z.Nodes, key)
				}
			}
			for zeff > 0 {
				zeff--
				var zdaf string
				var zpks []idx.Node
				zdaf, err = dc.ReadString()
				if err != nil {
					return
				}
				var zrsw uint32
				zrsw, err = dc.ReadArrayHeader()
				if err != nil {
					return
				}
				if cap(zpks) >= int(zrsw) {
					zpks = (zpks)[:zrsw]
				} else {
					zpks = make([]idx.Node, zrsw)
				}
				for zjfb := range zpks {
					err = zpks[zjfb].DecodeMsg(dc)
					if err != nil {
						return
					}
				}
				z.Nodes[zdaf] = zpks
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
		return err
	}
	err = en.WriteMapHeader(uint32(len(z.Nodes)))
	if err != nil {
		return
	}
	for zdaf, zpks := range z.Nodes {
		err = en.WriteString(zdaf)
		if err != nil {
			return
		}
		err = en.WriteArrayHeader(uint32(len(zpks)))
		if err != nil {
			return
		}
		for zjfb := range zpks {
			err = zpks[zjfb].EncodeMsg(en)
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
	for zdaf, zpks := range z.Nodes {
		o = msgp.AppendString(o, zdaf)
		o = msgp.AppendArrayHeader(o, uint32(len(zpks)))
		for zjfb := range zpks {
			o, err = zpks[zjfb].MarshalMsg(o)
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
	var zxpk uint32
	zxpk, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		return
	}
	for zxpk > 0 {
		zxpk--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "Nodes":
			var zdnj uint32
			zdnj, bts, err = msgp.ReadMapHeaderBytes(bts)
			if err != nil {
				return
			}
			if z.Nodes == nil && zdnj > 0 {
				z.Nodes = make(map[string][]idx.Node, zdnj)
			} else if len(z.Nodes) > 0 {
				for key, _ := range z.Nodes {
					delete(z.Nodes, key)
				}
			}
			for zdnj > 0 {
				var zdaf string
				var zpks []idx.Node
				zdnj--
				zdaf, bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					return
				}
				var zobc uint32
				zobc, bts, err = msgp.ReadArrayHeaderBytes(bts)
				if err != nil {
					return
				}
				if cap(zpks) >= int(zobc) {
					zpks = (zpks)[:zobc]
				} else {
					zpks = make([]idx.Node, zobc)
				}
				for zjfb := range zpks {
					bts, err = zpks[zjfb].UnmarshalMsg(bts)
					if err != nil {
						return
					}
				}
				z.Nodes[zdaf] = zpks
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
		for zdaf, zpks := range z.Nodes {
			_ = zpks
			s += msgp.StringPrefixSize + len(zdaf) + msgp.ArrayHeaderSize
			for zjfb := range zpks {
				s += zpks[zjfb].Msgsize()
			}
		}
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *IndexTagDetailsResp) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zema uint32
	zema, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zema > 0 {
		zema--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "Values":
			var zpez uint32
			zpez, err = dc.ReadMapHeader()
			if err != nil {
				return
			}
			if z.Values == nil && zpez > 0 {
				z.Values = make(map[string]uint64, zpez)
			} else if len(z.Values) > 0 {
				for key, _ := range z.Values {
					delete(z.Values, key)
				}
			}
			for zpez > 0 {
				zpez--
				var zsnv string
				var zkgt uint64
				zsnv, err = dc.ReadString()
				if err != nil {
					return
				}
				zkgt, err = dc.ReadUint64()
				if err != nil {
					return
				}
				z.Values[zsnv] = zkgt
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
		return err
	}
	err = en.WriteMapHeader(uint32(len(z.Values)))
	if err != nil {
		return
	}
	for zsnv, zkgt := range z.Values {
		err = en.WriteString(zsnv)
		if err != nil {
			return
		}
		err = en.WriteUint64(zkgt)
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
	for zsnv, zkgt := range z.Values {
		o = msgp.AppendString(o, zsnv)
		o = msgp.AppendUint64(o, zkgt)
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *IndexTagDetailsResp) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zqke uint32
	zqke, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		return
	}
	for zqke > 0 {
		zqke--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "Values":
			var zqyh uint32
			zqyh, bts, err = msgp.ReadMapHeaderBytes(bts)
			if err != nil {
				return
			}
			if z.Values == nil && zqyh > 0 {
				z.Values = make(map[string]uint64, zqyh)
			} else if len(z.Values) > 0 {
				for key, _ := range z.Values {
					delete(z.Values, key)
				}
			}
			for zqyh > 0 {
				var zsnv string
				var zkgt uint64
				zqyh--
				zsnv, bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					return
				}
				zkgt, bts, err = msgp.ReadUint64Bytes(bts)
				if err != nil {
					return
				}
				z.Values[zsnv] = zkgt
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
		for zsnv, zkgt := range z.Values {
			_ = zkgt
			s += msgp.StringPrefixSize + len(zsnv) + msgp.Uint64Size
		}
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *IndexTagsResp) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zywj uint32
	zywj, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zywj > 0 {
		zywj--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "Tags":
			var zjpj uint32
			zjpj, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.Tags) >= int(zjpj) {
				z.Tags = (z.Tags)[:zjpj]
			} else {
				z.Tags = make([]string, zjpj)
			}
			for zyzr := range z.Tags {
				z.Tags[zyzr], err = dc.ReadString()
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
		return err
	}
	err = en.WriteArrayHeader(uint32(len(z.Tags)))
	if err != nil {
		return
	}
	for zyzr := range z.Tags {
		err = en.WriteString(z.Tags[zyzr])
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
	for zyzr := range z.Tags {
		o = msgp.AppendString(o, z.Tags[zyzr])
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *IndexTagsResp) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zzpf uint32
	zzpf, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		return
	}
	for zzpf > 0 {
		zzpf--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "Tags":
			var zrfe uint32
			zrfe, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				return
			}
			if cap(z.Tags) >= int(zrfe) {
				z.Tags = (z.Tags)[:zrfe]
			} else {
				z.Tags = make([]string, zrfe)
			}
			for zyzr := range z.Tags {
				z.Tags[zyzr], bts, err = msgp.ReadStringBytes(bts)
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
	for zyzr := range z.Tags {
		s += msgp.StringPrefixSize + len(z.Tags[zyzr])
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *MetricsDeleteResp) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zgmo uint32
	zgmo, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zgmo > 0 {
		zgmo--
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
		return err
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
	var ztaf uint32
	ztaf, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		return
	}
	for ztaf > 0 {
		ztaf--
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
