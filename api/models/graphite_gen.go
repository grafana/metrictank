package models

// NOTE: THIS FILE WAS PRODUCED BY THE
// MSGP CODE GENERATION TOOL (github.com/tinylib/msgp)
// DO NOT EDIT

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *SeriesPickle) DecodeMsg(dc *msgp.Reader) (err error) {
	var zb0002 uint32
	zb0002, err = dc.ReadArrayHeader()
	if err != nil {
		return
	}
	if cap((*z)) >= int(zb0002) {
		(*z) = (*z)[:zb0002]
	} else {
		(*z) = make(SeriesPickle, zb0002)
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
func (z SeriesPickle) EncodeMsg(en *msgp.Writer) (err error) {
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
func (z SeriesPickle) MarshalMsg(b []byte) (o []byte, err error) {
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
func (z *SeriesPickle) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var zb0002 uint32
	zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		return
	}
	if cap((*z)) >= int(zb0002) {
		(*z) = (*z)[:zb0002]
	} else {
		(*z) = make(SeriesPickle, zb0002)
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
func (z SeriesPickle) Msgsize() (s int) {
	s = msgp.ArrayHeaderSize
	for zb0003 := range z {
		s += z[zb0003].Msgsize()
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *SeriesPickleItem) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "path":
			z.Path, err = dc.ReadString()
			if err != nil {
				return
			}
		case "isLeaf":
			z.IsLeaf, err = dc.ReadBool()
			if err != nil {
				return
			}
		case "intervals":
			var zb0002 uint32
			zb0002, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.Intervals) >= int(zb0002) {
				z.Intervals = (z.Intervals)[:zb0002]
			} else {
				z.Intervals = make([][]int64, zb0002)
			}
			for za0001 := range z.Intervals {
				var zb0003 uint32
				zb0003, err = dc.ReadArrayHeader()
				if err != nil {
					return
				}
				if cap(z.Intervals[za0001]) >= int(zb0003) {
					z.Intervals[za0001] = (z.Intervals[za0001])[:zb0003]
				} else {
					z.Intervals[za0001] = make([]int64, zb0003)
				}
				for za0002 := range z.Intervals[za0001] {
					z.Intervals[za0001][za0002], err = dc.ReadInt64()
					if err != nil {
						return
					}
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
func (z *SeriesPickleItem) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 3
	// write "path"
	err = en.Append(0x83, 0xa4, 0x70, 0x61, 0x74, 0x68)
	if err != nil {
		return
	}
	err = en.WriteString(z.Path)
	if err != nil {
		return
	}
	// write "isLeaf"
	err = en.Append(0xa6, 0x69, 0x73, 0x4c, 0x65, 0x61, 0x66)
	if err != nil {
		return
	}
	err = en.WriteBool(z.IsLeaf)
	if err != nil {
		return
	}
	// write "intervals"
	err = en.Append(0xa9, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x76, 0x61, 0x6c, 0x73)
	if err != nil {
		return
	}
	err = en.WriteArrayHeader(uint32(len(z.Intervals)))
	if err != nil {
		return
	}
	for za0001 := range z.Intervals {
		err = en.WriteArrayHeader(uint32(len(z.Intervals[za0001])))
		if err != nil {
			return
		}
		for za0002 := range z.Intervals[za0001] {
			err = en.WriteInt64(z.Intervals[za0001][za0002])
			if err != nil {
				return
			}
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *SeriesPickleItem) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 3
	// string "path"
	o = append(o, 0x83, 0xa4, 0x70, 0x61, 0x74, 0x68)
	o = msgp.AppendString(o, z.Path)
	// string "isLeaf"
	o = append(o, 0xa6, 0x69, 0x73, 0x4c, 0x65, 0x61, 0x66)
	o = msgp.AppendBool(o, z.IsLeaf)
	// string "intervals"
	o = append(o, 0xa9, 0x69, 0x6e, 0x74, 0x65, 0x72, 0x76, 0x61, 0x6c, 0x73)
	o = msgp.AppendArrayHeader(o, uint32(len(z.Intervals)))
	for za0001 := range z.Intervals {
		o = msgp.AppendArrayHeader(o, uint32(len(z.Intervals[za0001])))
		for za0002 := range z.Intervals[za0001] {
			o = msgp.AppendInt64(o, z.Intervals[za0001][za0002])
		}
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *SeriesPickleItem) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "path":
			z.Path, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		case "isLeaf":
			z.IsLeaf, bts, err = msgp.ReadBoolBytes(bts)
			if err != nil {
				return
			}
		case "intervals":
			var zb0002 uint32
			zb0002, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				return
			}
			if cap(z.Intervals) >= int(zb0002) {
				z.Intervals = (z.Intervals)[:zb0002]
			} else {
				z.Intervals = make([][]int64, zb0002)
			}
			for za0001 := range z.Intervals {
				var zb0003 uint32
				zb0003, bts, err = msgp.ReadArrayHeaderBytes(bts)
				if err != nil {
					return
				}
				if cap(z.Intervals[za0001]) >= int(zb0003) {
					z.Intervals[za0001] = (z.Intervals[za0001])[:zb0003]
				} else {
					z.Intervals[za0001] = make([]int64, zb0003)
				}
				for za0002 := range z.Intervals[za0001] {
					z.Intervals[za0001][za0002], bts, err = msgp.ReadInt64Bytes(bts)
					if err != nil {
						return
					}
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
func (z *SeriesPickleItem) Msgsize() (s int) {
	s = 1 + 5 + msgp.StringPrefixSize + len(z.Path) + 7 + msgp.BoolSize + 10 + msgp.ArrayHeaderSize
	for za0001 := range z.Intervals {
		s += msgp.ArrayHeaderSize + (len(z.Intervals[za0001]) * (msgp.Int64Size))
	}
	return
}
