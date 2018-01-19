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
				for key := range z.Nodes {
					delete(z.Nodes, key)
				}
			}
			for msz > 0 {
				msz--
				var bzg string
				var bai []idx.Node
				bzg, err = dc.ReadString()
				if err != nil {
					return
				}
				var xsz uint32
				xsz, err = dc.ReadArrayHeader()
				if err != nil {
					return
				}
				if cap(bai) >= int(xsz) {
					bai = bai[:xsz]
				} else {
					bai = make([]idx.Node, xsz)
				}
				for cmr := range bai {
					err = bai[cmr].DecodeMsg(dc)
					if err != nil {
						return
					}
				}
				z.Nodes[bzg] = bai
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
	for bzg, bai := range z.Nodes {
		err = en.WriteString(bzg)
		if err != nil {
			return
		}
		err = en.WriteArrayHeader(uint32(len(bai)))
		if err != nil {
			return
		}
		for cmr := range bai {
			err = bai[cmr].EncodeMsg(en)
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
	for bzg, bai := range z.Nodes {
		o = msgp.AppendString(o, bzg)
		o = msgp.AppendArrayHeader(o, uint32(len(bai)))
		for cmr := range bai {
			o, err = bai[cmr].MarshalMsg(o)
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
				for key := range z.Nodes {
					delete(z.Nodes, key)
				}
			}
			for msz > 0 {
				var bzg string
				var bai []idx.Node
				msz--
				bzg, bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					return
				}
				var xsz uint32
				xsz, bts, err = msgp.ReadArrayHeaderBytes(bts)
				if err != nil {
					return
				}
				if cap(bai) >= int(xsz) {
					bai = bai[:xsz]
				} else {
					bai = make([]idx.Node, xsz)
				}
				for cmr := range bai {
					bts, err = bai[cmr].UnmarshalMsg(bts)
					if err != nil {
						return
					}
				}
				z.Nodes[bzg] = bai
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
		for bzg, bai := range z.Nodes {
			_ = bai
			s += msgp.StringPrefixSize + len(bzg) + msgp.ArrayHeaderSize
			for cmr := range bai {
				s += bai[cmr].Msgsize()
			}
		}
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *IndexTagFindSeriesResp) DecodeMsg(dc *msgp.Reader) (err error) {
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
				z.Series = make([]string, xsz)
			}
			for ajw := range z.Series {
				z.Series[ajw], err = dc.ReadString()
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
func (z *IndexTagFindSeriesResp) EncodeMsg(en *msgp.Writer) (err error) {
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
	for ajw := range z.Series {
		err = en.WriteString(z.Series[ajw])
		if err != nil {
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *IndexTagFindSeriesResp) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 1
	// string "Series"
	o = append(o, 0x81, 0xa6, 0x53, 0x65, 0x72, 0x69, 0x65, 0x73)
	o = msgp.AppendArrayHeader(o, uint32(len(z.Series)))
	for ajw := range z.Series {
		o = msgp.AppendString(o, z.Series[ajw])
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *IndexTagFindSeriesResp) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
				z.Series = make([]string, xsz)
			}
			for ajw := range z.Series {
				z.Series[ajw], bts, err = msgp.ReadStringBytes(bts)
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

func (z *IndexTagFindSeriesResp) Msgsize() (s int) {
	s = 1 + 7 + msgp.ArrayHeaderSize
	for ajw := range z.Series {
		s += msgp.StringPrefixSize + len(z.Series[ajw])
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *IndexTagListResp) DecodeMsg(dc *msgp.Reader) (err error) {
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
			for wht := range z.Tags {
				z.Tags[wht], err = dc.ReadString()
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
func (z *IndexTagListResp) EncodeMsg(en *msgp.Writer) (err error) {
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
	for wht := range z.Tags {
		err = en.WriteString(z.Tags[wht])
		if err != nil {
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *IndexTagListResp) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 1
	// string "Tags"
	o = append(o, 0x81, 0xa4, 0x54, 0x61, 0x67, 0x73)
	o = msgp.AppendArrayHeader(o, uint32(len(z.Tags)))
	for wht := range z.Tags {
		o = msgp.AppendString(o, z.Tags[wht])
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *IndexTagListResp) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
			for wht := range z.Tags {
				z.Tags[wht], bts, err = msgp.ReadStringBytes(bts)
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

func (z *IndexTagListResp) Msgsize() (s int) {
	s = 1 + 5 + msgp.ArrayHeaderSize
	for wht := range z.Tags {
		s += msgp.StringPrefixSize + len(z.Tags[wht])
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *IndexTagResp) DecodeMsg(dc *msgp.Reader) (err error) {
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
				z.Values = make(map[string]uint32, msz)
			} else if len(z.Values) > 0 {
				for key, _ := range z.Values {
					delete(z.Values, key)
				}
			}
			for msz > 0 {
				msz--
				var hct string
				var cua uint32
				hct, err = dc.ReadString()
				if err != nil {
					return
				}
				cua, err = dc.ReadUint32()
				if err != nil {
					return
				}
				z.Values[hct] = cua
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
func (z *IndexTagResp) EncodeMsg(en *msgp.Writer) (err error) {
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
	for hct, cua := range z.Values {
		err = en.WriteString(hct)
		if err != nil {
			return
		}
		err = en.WriteUint32(cua)
		if err != nil {
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *IndexTagResp) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 1
	// string "Values"
	o = append(o, 0x81, 0xa6, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x73)
	o = msgp.AppendMapHeader(o, uint32(len(z.Values)))
	for hct, cua := range z.Values {
		o = msgp.AppendString(o, hct)
		o = msgp.AppendUint32(o, cua)
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *IndexTagResp) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
				z.Values = make(map[string]uint32, msz)
			} else if len(z.Values) > 0 {
				for key, _ := range z.Values {
					delete(z.Values, key)
				}
			}
			for msz > 0 {
				var hct string
				var cua uint32
				msz--
				hct, bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					return
				}
				cua, bts, err = msgp.ReadUint32Bytes(bts)
				if err != nil {
					return
				}
				z.Values[hct] = cua
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

func (z *IndexTagResp) Msgsize() (s int) {
	s = 1 + 7 + msgp.MapHeaderSize
	if z.Values != nil {
		for hct, cua := range z.Values {
			_ = cua
			s += msgp.StringPrefixSize + len(hct) + msgp.Uint32Size
		}
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
