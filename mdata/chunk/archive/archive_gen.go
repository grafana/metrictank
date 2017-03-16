package archive

// NOTE: THIS FILE WAS PRODUCED BY THE
// MSGP CODE GENERATION TOOL (github.com/tinylib/msgp)
// DO NOT EDIT

import (
	"github.com/raintank/metrictank/mdata/chunk"
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *Archive) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "RowKey":
			z.RowKey, err = dc.ReadString()
			if err != nil {
				return
			}
		case "SecondsPerPoint":
			z.SecondsPerPoint, err = dc.ReadUint32()
			if err != nil {
				return
			}
		case "Points":
			z.Points, err = dc.ReadUint32()
			if err != nil {
				return
			}
		case "Chunks":
			var zbai uint32
			zbai, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.Chunks) >= int(zbai) {
				z.Chunks = (z.Chunks)[:zbai]
			} else {
				z.Chunks = make([]chunk.IterGen, zbai)
			}
			for zxvk := range z.Chunks {
				err = z.Chunks[zxvk].DecodeMsg(dc)
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
func (z *Archive) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 4
	// write "RowKey"
	err = en.Append(0x84, 0xa6, 0x52, 0x6f, 0x77, 0x4b, 0x65, 0x79)
	if err != nil {
		return err
	}
	err = en.WriteString(z.RowKey)
	if err != nil {
		return
	}
	// write "SecondsPerPoint"
	err = en.Append(0xaf, 0x53, 0x65, 0x63, 0x6f, 0x6e, 0x64, 0x73, 0x50, 0x65, 0x72, 0x50, 0x6f, 0x69, 0x6e, 0x74)
	if err != nil {
		return err
	}
	err = en.WriteUint32(z.SecondsPerPoint)
	if err != nil {
		return
	}
	// write "Points"
	err = en.Append(0xa6, 0x50, 0x6f, 0x69, 0x6e, 0x74, 0x73)
	if err != nil {
		return err
	}
	err = en.WriteUint32(z.Points)
	if err != nil {
		return
	}
	// write "Chunks"
	err = en.Append(0xa6, 0x43, 0x68, 0x75, 0x6e, 0x6b, 0x73)
	if err != nil {
		return err
	}
	err = en.WriteArrayHeader(uint32(len(z.Chunks)))
	if err != nil {
		return
	}
	for zxvk := range z.Chunks {
		err = z.Chunks[zxvk].EncodeMsg(en)
		if err != nil {
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *Archive) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 4
	// string "RowKey"
	o = append(o, 0x84, 0xa6, 0x52, 0x6f, 0x77, 0x4b, 0x65, 0x79)
	o = msgp.AppendString(o, z.RowKey)
	// string "SecondsPerPoint"
	o = append(o, 0xaf, 0x53, 0x65, 0x63, 0x6f, 0x6e, 0x64, 0x73, 0x50, 0x65, 0x72, 0x50, 0x6f, 0x69, 0x6e, 0x74)
	o = msgp.AppendUint32(o, z.SecondsPerPoint)
	// string "Points"
	o = append(o, 0xa6, 0x50, 0x6f, 0x69, 0x6e, 0x74, 0x73)
	o = msgp.AppendUint32(o, z.Points)
	// string "Chunks"
	o = append(o, 0xa6, 0x43, 0x68, 0x75, 0x6e, 0x6b, 0x73)
	o = msgp.AppendArrayHeader(o, uint32(len(z.Chunks)))
	for zxvk := range z.Chunks {
		o, err = z.Chunks[zxvk].MarshalMsg(o)
		if err != nil {
			return
		}
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Archive) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "RowKey":
			z.RowKey, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		case "SecondsPerPoint":
			z.SecondsPerPoint, bts, err = msgp.ReadUint32Bytes(bts)
			if err != nil {
				return
			}
		case "Points":
			z.Points, bts, err = msgp.ReadUint32Bytes(bts)
			if err != nil {
				return
			}
		case "Chunks":
			var zajw uint32
			zajw, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				return
			}
			if cap(z.Chunks) >= int(zajw) {
				z.Chunks = (z.Chunks)[:zajw]
			} else {
				z.Chunks = make([]chunk.IterGen, zajw)
			}
			for zxvk := range z.Chunks {
				bts, err = z.Chunks[zxvk].UnmarshalMsg(bts)
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
func (z *Archive) Msgsize() (s int) {
	s = 1 + 7 + msgp.StringPrefixSize + len(z.RowKey) + 16 + msgp.Uint32Size + 7 + msgp.Uint32Size + 7 + msgp.ArrayHeaderSize
	for zxvk := range z.Chunks {
		s += z.Chunks[zxvk].Msgsize()
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *Metric) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "MetricData":
			err = z.MetricData.DecodeMsg(dc)
			if err != nil {
				return
			}
		case "AggregationMethod":
			z.AggregationMethod, err = dc.ReadUint32()
			if err != nil {
				return
			}
		case "Archives":
			var zcua uint32
			zcua, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.Archives) >= int(zcua) {
				z.Archives = (z.Archives)[:zcua]
			} else {
				z.Archives = make([]Archive, zcua)
			}
			for zwht := range z.Archives {
				err = z.Archives[zwht].DecodeMsg(dc)
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
func (z *Metric) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 3
	// write "MetricData"
	err = en.Append(0x83, 0xaa, 0x4d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x44, 0x61, 0x74, 0x61)
	if err != nil {
		return err
	}
	err = z.MetricData.EncodeMsg(en)
	if err != nil {
		return
	}
	// write "AggregationMethod"
	err = en.Append(0xb1, 0x41, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x4d, 0x65, 0x74, 0x68, 0x6f, 0x64)
	if err != nil {
		return err
	}
	err = en.WriteUint32(z.AggregationMethod)
	if err != nil {
		return
	}
	// write "Archives"
	err = en.Append(0xa8, 0x41, 0x72, 0x63, 0x68, 0x69, 0x76, 0x65, 0x73)
	if err != nil {
		return err
	}
	err = en.WriteArrayHeader(uint32(len(z.Archives)))
	if err != nil {
		return
	}
	for zwht := range z.Archives {
		err = z.Archives[zwht].EncodeMsg(en)
		if err != nil {
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *Metric) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 3
	// string "MetricData"
	o = append(o, 0x83, 0xaa, 0x4d, 0x65, 0x74, 0x72, 0x69, 0x63, 0x44, 0x61, 0x74, 0x61)
	o, err = z.MetricData.MarshalMsg(o)
	if err != nil {
		return
	}
	// string "AggregationMethod"
	o = append(o, 0xb1, 0x41, 0x67, 0x67, 0x72, 0x65, 0x67, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x4d, 0x65, 0x74, 0x68, 0x6f, 0x64)
	o = msgp.AppendUint32(o, z.AggregationMethod)
	// string "Archives"
	o = append(o, 0xa8, 0x41, 0x72, 0x63, 0x68, 0x69, 0x76, 0x65, 0x73)
	o = msgp.AppendArrayHeader(o, uint32(len(z.Archives)))
	for zwht := range z.Archives {
		o, err = z.Archives[zwht].MarshalMsg(o)
		if err != nil {
			return
		}
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Metric) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "MetricData":
			bts, err = z.MetricData.UnmarshalMsg(bts)
			if err != nil {
				return
			}
		case "AggregationMethod":
			z.AggregationMethod, bts, err = msgp.ReadUint32Bytes(bts)
			if err != nil {
				return
			}
		case "Archives":
			var zlqf uint32
			zlqf, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				return
			}
			if cap(z.Archives) >= int(zlqf) {
				z.Archives = (z.Archives)[:zlqf]
			} else {
				z.Archives = make([]Archive, zlqf)
			}
			for zwht := range z.Archives {
				bts, err = z.Archives[zwht].UnmarshalMsg(bts)
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
func (z *Metric) Msgsize() (s int) {
	s = 1 + 11 + z.MetricData.Msgsize() + 18 + msgp.Uint32Size + 9 + msgp.ArrayHeaderSize
	for zwht := range z.Archives {
		s += z.Archives[zwht].Msgsize()
	}
	return
}
