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
		case "Series":
			var xsz uint32
			xsz, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.Series) >= int(xsz) {
				z.Series = z.Series[:xsz]
			} else {
				z.Series = make([]Series, xsz)
			}
			for xvk := range z.Series {
				err = z.Series[xvk].DecodeMsg(dc)
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
	for xvk := range z.Series {
		err = z.Series[xvk].EncodeMsg(en)
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
	for xvk := range z.Series {
		o, err = z.Series[xvk].MarshalMsg(o)
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
		case "Series":
			var xsz uint32
			xsz, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				return
			}
			if cap(z.Series) >= int(xsz) {
				z.Series = z.Series[:xsz]
			} else {
				z.Series = make([]Series, xsz)
			}
			for xvk := range z.Series {
				bts, err = z.Series[xvk].UnmarshalMsg(bts)
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

func (z *GetDataResp) Msgsize() (s int) {
	s = 1 + 7 + msgp.ArrayHeaderSize
	for xvk := range z.Series {
		s += z.Series[xvk].Msgsize()
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *IndexFindByTagResp) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "Metrics":
			var xsz uint32
			xsz, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.Metrics) >= int(xsz) {
				z.Metrics = z.Metrics[:xsz]
			} else {
				z.Metrics = make([]idx.Node, xsz)
			}
			for bzg := range z.Metrics {
				err = z.Metrics[bzg].DecodeMsg(dc)
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
	for bzg := range z.Metrics {
		err = z.Metrics[bzg].EncodeMsg(en)
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
	for bzg := range z.Metrics {
		o, err = z.Metrics[bzg].MarshalMsg(o)
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
		case "Metrics":
			var xsz uint32
			xsz, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				return
			}
			if cap(z.Metrics) >= int(xsz) {
				z.Metrics = z.Metrics[:xsz]
			} else {
				z.Metrics = make([]idx.Node, xsz)
			}
			for bzg := range z.Metrics {
				bts, err = z.Metrics[bzg].UnmarshalMsg(bts)
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

func (z *IndexFindByTagResp) Msgsize() (s int) {
	s = 1 + 8 + msgp.ArrayHeaderSize
	for bzg := range z.Metrics {
		s += z.Metrics[bzg].Msgsize()
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *IndexFindResp) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "Nodes":
			var msz uint32
			msz, err = dc.ReadMapHeader()
			if err != nil {
				return
			}
			if z.Nodes == nil && msz > 0 {
				z.Nodes = make(map[string][]idx.Node, msz)
			} else if len(z.Nodes) > 0 {
				for key, _ := range z.Nodes {
					delete(z.Nodes, key)
				}
			}
			for msz > 0 {
				msz--
				var bai string
				var cmr []idx.Node
				bai, err = dc.ReadString()
				if err != nil {
					return
				}
				var xsz uint32
				xsz, err = dc.ReadArrayHeader()
				if err != nil {
					return
				}
				if cap(cmr) >= int(xsz) {
					cmr = cmr[:xsz]
				} else {
					cmr = make([]idx.Node, xsz)
				}
				for ajw := range cmr {
					err = cmr[ajw].DecodeMsg(dc)
					if err != nil {
						return
					}
				}
				z.Nodes[bai] = cmr
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
	for bai, cmr := range z.Nodes {
		err = en.WriteString(bai)
		if err != nil {
			return
		}
		err = en.WriteArrayHeader(uint32(len(cmr)))
		if err != nil {
			return
		}
		for ajw := range cmr {
			err = cmr[ajw].EncodeMsg(en)
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
	for bai, cmr := range z.Nodes {
		o = msgp.AppendString(o, bai)
		o = msgp.AppendArrayHeader(o, uint32(len(cmr)))
		for ajw := range cmr {
			o, err = cmr[ajw].MarshalMsg(o)
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
		case "Nodes":
			var msz uint32
			msz, bts, err = msgp.ReadMapHeaderBytes(bts)
			if err != nil {
				return
			}
			if z.Nodes == nil && msz > 0 {
				z.Nodes = make(map[string][]idx.Node, msz)
			} else if len(z.Nodes) > 0 {
				for key, _ := range z.Nodes {
					delete(z.Nodes, key)
				}
			}
			for msz > 0 {
				var bai string
				var cmr []idx.Node
				msz--
				bai, bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					return
				}
				var xsz uint32
				xsz, bts, err = msgp.ReadArrayHeaderBytes(bts)
				if err != nil {
					return
				}
				if cap(cmr) >= int(xsz) {
					cmr = cmr[:xsz]
				} else {
					cmr = make([]idx.Node, xsz)
				}
				for ajw := range cmr {
					bts, err = cmr[ajw].UnmarshalMsg(bts)
					if err != nil {
						return
					}
				}
				z.Nodes[bai] = cmr
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

func (z *IndexFindResp) Msgsize() (s int) {
	s = 1 + 6 + msgp.MapHeaderSize
	if z.Nodes != nil {
		for bai, cmr := range z.Nodes {
			_ = cmr
			s += msgp.StringPrefixSize + len(bai) + msgp.ArrayHeaderSize
			for ajw := range cmr {
				s += cmr[ajw].Msgsize()
			}
		}
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *IndexTagDetailsResp) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "Values":
			var msz uint32
			msz, err = dc.ReadMapHeader()
			if err != nil {
				return
			}
			if z.Values == nil && msz > 0 {
				z.Values = make(map[string]uint64, msz)
			} else if len(z.Values) > 0 {
				for key, _ := range z.Values {
					delete(z.Values, key)
				}
			}
			for msz > 0 {
				msz--
				var wht string
				var hct uint64
				wht, err = dc.ReadString()
				if err != nil {
					return
				}
				hct, err = dc.ReadUint64()
				if err != nil {
					return
				}
				z.Values[wht] = hct
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
	for wht, hct := range z.Values {
		err = en.WriteString(wht)
		if err != nil {
			return
		}
		err = en.WriteUint64(hct)
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
	for wht, hct := range z.Values {
		o = msgp.AppendString(o, wht)
		o = msgp.AppendUint64(o, hct)
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *IndexTagDetailsResp) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "Values":
			var msz uint32
			msz, bts, err = msgp.ReadMapHeaderBytes(bts)
			if err != nil {
				return
			}
			if z.Values == nil && msz > 0 {
				z.Values = make(map[string]uint64, msz)
			} else if len(z.Values) > 0 {
				for key, _ := range z.Values {
					delete(z.Values, key)
				}
			}
			for msz > 0 {
				var wht string
				var hct uint64
				msz--
				wht, bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					return
				}
				hct, bts, err = msgp.ReadUint64Bytes(bts)
				if err != nil {
					return
				}
				z.Values[wht] = hct
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

func (z *IndexTagDetailsResp) Msgsize() (s int) {
	s = 1 + 7 + msgp.MapHeaderSize
	if z.Values != nil {
		for wht, hct := range z.Values {
			_ = hct
			s += msgp.StringPrefixSize + len(wht) + msgp.Uint64Size
		}
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *IndexTagsResp) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "Tags":
			var xsz uint32
			xsz, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.Tags) >= int(xsz) {
				z.Tags = z.Tags[:xsz]
			} else {
				z.Tags = make([]string, xsz)
			}
			for cua := range z.Tags {
				z.Tags[cua], err = dc.ReadString()
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
	for cua := range z.Tags {
		err = en.WriteString(z.Tags[cua])
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
	for cua := range z.Tags {
		o = msgp.AppendString(o, z.Tags[cua])
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *IndexTagsResp) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "Tags":
			var xsz uint32
			xsz, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				return
			}
			if cap(z.Tags) >= int(xsz) {
				z.Tags = z.Tags[:xsz]
			} else {
				z.Tags = make([]string, xsz)
			}
			for cua := range z.Tags {
				z.Tags[cua], bts, err = msgp.ReadStringBytes(bts)
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

func (z *IndexTagsResp) Msgsize() (s int) {
	s = 1 + 5 + msgp.ArrayHeaderSize
	for cua := range z.Tags {
		s += msgp.StringPrefixSize + len(z.Tags[cua])
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *MetricsDeleteResp) DecodeMsg(dc *msgp.Reader) (err error) {
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

func (z MetricsDeleteResp) Msgsize() (s int) {
	s = 1 + 12 + msgp.IntSize
	return
}

// DecodeMsg implements msgp.Decodable
func (z *StringList) DecodeMsg(dc *msgp.Reader) (err error) {
	var xsz uint32
	xsz, err = dc.ReadArrayHeader()
	if err != nil {
		return
	}
	if cap((*z)) >= int(xsz) {
		(*z) = (*z)[:xsz]
	} else {
		(*z) = make(StringList, xsz)
	}
	for lqf := range *z {
		(*z)[lqf], err = dc.ReadString()
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
	for daf := range z {
		err = en.WriteString(z[daf])
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
	for daf := range z {
		o = msgp.AppendString(o, z[daf])
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *StringList) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var xsz uint32
	xsz, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		return
	}
	if cap((*z)) >= int(xsz) {
		(*z) = (*z)[:xsz]
	} else {
		(*z) = make(StringList, xsz)
	}
	for pks := range *z {
		(*z)[pks], bts, err = msgp.ReadStringBytes(bts)
		if err != nil {
			return
		}
	}
	o = bts
	return
}

func (z StringList) Msgsize() (s int) {
	s = msgp.ArrayHeaderSize
	for jfb := range z {
		s += msgp.StringPrefixSize + len(z[jfb])
	}
	return
}
