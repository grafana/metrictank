package main

// NOTE: THIS FILE WAS PRODUCED BY THE
// MSGP CODE GENERATION TOOL (github.com/tinylib/msgp)
// DO NOT EDIT

import (
	_ "net/http/pprof"

	"github.com/tinylib/msgp/msgp"
	"gopkg.in/raintank/schema.v1"
)

// DecodeMsg implements msgp.Decodable
func (z *Series) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "Target":
			z.Target, err = dc.ReadString()
			if err != nil {
				return
			}
		case "Datapoints":
			var zbai uint32
			zbai, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.Datapoints) >= int(zbai) {
				z.Datapoints = (z.Datapoints)[:zbai]
			} else {
				z.Datapoints = make([]schema.Point, zbai)
			}
			for zxvk := range z.Datapoints {
				err = z.Datapoints[zxvk].DecodeMsg(dc)
				if err != nil {
					return
				}
			}
		case "Interval":
			z.Interval, err = dc.ReadUint32()
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
	// map header, size 3
	// write "Target"
	err = en.Append(0x83, 0xa6, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74)
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
	// write "Interval"
	err = en.Append(0xa8, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x76, 0x61, 0x6c)
	if err != nil {
		return err
	}
	err = en.WriteUint32(z.Interval)
	if err != nil {
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *Series) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 3
	// string "Target"
	o = append(o, 0x83, 0xa6, 0x54, 0x61, 0x72, 0x67, 0x65, 0x74)
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
	// string "Interval"
	o = append(o, 0xa8, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x76, 0x61, 0x6c)
	o = msgp.AppendUint32(o, z.Interval)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *Series) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "Target":
			z.Target, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		case "Datapoints":
			var zajw uint32
			zajw, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				return
			}
			if cap(z.Datapoints) >= int(zajw) {
				z.Datapoints = (z.Datapoints)[:zajw]
			} else {
				z.Datapoints = make([]schema.Point, zajw)
			}
			for zxvk := range z.Datapoints {
				bts, err = z.Datapoints[zxvk].UnmarshalMsg(bts)
				if err != nil {
					return
				}
			}
		case "Interval":
			z.Interval, bts, err = msgp.ReadUint32Bytes(bts)
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
	s += 9 + msgp.Uint32Size
	return
}

// DecodeMsg implements msgp.Decodable
func (z *SeriesByTarget) DecodeMsg(dc *msgp.Reader) (err error) {
	var zcua uint32
	zcua, err = dc.ReadArrayHeader()
	if err != nil {
		return
	}
	if cap((*z)) >= int(zcua) {
		(*z) = (*z)[:zcua]
	} else {
		(*z) = make(SeriesByTarget, zcua)
	}
	for zhct := range *z {
		err = (*z)[zhct].DecodeMsg(dc)
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
	for zxhx := range z {
		err = z[zxhx].EncodeMsg(en)
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
	for zxhx := range z {
		o, err = z[zxhx].MarshalMsg(o)
		if err != nil {
			return
		}
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *SeriesByTarget) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var zdaf uint32
	zdaf, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		return
	}
	if cap((*z)) >= int(zdaf) {
		(*z) = (*z)[:zdaf]
	} else {
		(*z) = make(SeriesByTarget, zdaf)
	}
	for zlqf := range *z {
		bts, err = (*z)[zlqf].UnmarshalMsg(bts)
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
	for zpks := range z {
		s += z[zpks].Msgsize()
	}
	return
}
