package idx

// NOTE: THIS FILE WAS PRODUCED BY THE
// MSGP CODE GENERATION TOOL (github.com/tinylib/msgp)
// DO NOT EDIT

import (
	"github.com/tinylib/msgp/msgp"
)

// DecodeMsg implements msgp.Decodable
func (z *MetricDefinition) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "Id":
			err = z.Id.DecodeMsg(dc)
			if err != nil {
				return
			}
		case "OrgId":
			z.OrgId, err = dc.ReadUint32()
			if err != nil {
				return
			}
		case "name":
			err = dc.ReadExtension(&z.Name)
			if err != nil {
				return
			}
		case "Interval":
			z.Interval, err = dc.ReadInt()
			if err != nil {
				return
			}
		case "Unit":
			z.Unit, err = dc.ReadString()
			if err != nil {
				return
			}
		case "tagkeyvalues":
			err = dc.ReadExtension(&z.Tags)
			if err != nil {
				return
			}
		case "LastUpdate":
			z.LastUpdate, err = dc.ReadInt64()
			if err != nil {
				return
			}
		case "Partition":
			z.Partition, err = dc.ReadInt32()
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
func (z *MetricDefinition) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 8
	// write "Id"
	err = en.Append(0x88, 0xa2, 0x49, 0x64)
	if err != nil {
		return err
	}
	err = z.Id.EncodeMsg(en)
	if err != nil {
		return
	}
	// write "OrgId"
	err = en.Append(0xa5, 0x4f, 0x72, 0x67, 0x49, 0x64)
	if err != nil {
		return err
	}
	err = en.WriteUint32(z.OrgId)
	if err != nil {
		return
	}
	// write "name"
	err = en.Append(0xa4, 0x6e, 0x61, 0x6d, 0x65)
	if err != nil {
		return err
	}
	err = en.WriteExtension(&z.Name)
	if err != nil {
		return
	}
	// write "Interval"
	err = en.Append(0xa8, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x76, 0x61, 0x6c)
	if err != nil {
		return err
	}
	err = en.WriteInt(z.Interval)
	if err != nil {
		return
	}
	// write "Unit"
	err = en.Append(0xa4, 0x55, 0x6e, 0x69, 0x74)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Unit)
	if err != nil {
		return
	}
	// write "tagkeyvalues"
	err = en.Append(0xac, 0x74, 0x61, 0x67, 0x6b, 0x65, 0x79, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73)
	if err != nil {
		return err
	}
	err = en.WriteExtension(&z.Tags)
	if err != nil {
		return
	}
	// write "LastUpdate"
	err = en.Append(0xaa, 0x4c, 0x61, 0x73, 0x74, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65)
	if err != nil {
		return err
	}
	err = en.WriteInt64(z.LastUpdate)
	if err != nil {
		return
	}
	// write "Partition"
	err = en.Append(0xa9, 0x50, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e)
	if err != nil {
		return err
	}
	err = en.WriteInt32(z.Partition)
	if err != nil {
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *MetricDefinition) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 8
	// string "Id"
	o = append(o, 0x88, 0xa2, 0x49, 0x64)
	o, err = z.Id.MarshalMsg(o)
	if err != nil {
		return
	}
	// string "OrgId"
	o = append(o, 0xa5, 0x4f, 0x72, 0x67, 0x49, 0x64)
	o = msgp.AppendUint32(o, z.OrgId)
	// string "name"
	o = append(o, 0xa4, 0x6e, 0x61, 0x6d, 0x65)
	o, err = msgp.AppendExtension(o, &z.Name)
	if err != nil {
		return
	}
	// string "Interval"
	o = append(o, 0xa8, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x76, 0x61, 0x6c)
	o = msgp.AppendInt(o, z.Interval)
	// string "Unit"
	o = append(o, 0xa4, 0x55, 0x6e, 0x69, 0x74)
	o = msgp.AppendString(o, z.Unit)
	// string "tagkeyvalues"
	o = append(o, 0xac, 0x74, 0x61, 0x67, 0x6b, 0x65, 0x79, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x73)
	o, err = msgp.AppendExtension(o, &z.Tags)
	if err != nil {
		return
	}
	// string "LastUpdate"
	o = append(o, 0xaa, 0x4c, 0x61, 0x73, 0x74, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65)
	o = msgp.AppendInt64(o, z.LastUpdate)
	// string "Partition"
	o = append(o, 0xa9, 0x50, 0x61, 0x72, 0x74, 0x69, 0x74, 0x69, 0x6f, 0x6e)
	o = msgp.AppendInt32(o, z.Partition)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *MetricDefinition) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "Id":
			bts, err = z.Id.UnmarshalMsg(bts)
			if err != nil {
				return
			}
		case "OrgId":
			z.OrgId, bts, err = msgp.ReadUint32Bytes(bts)
			if err != nil {
				return
			}
		case "name":
			bts, err = msgp.ReadExtensionBytes(bts, &z.Name)
			if err != nil {
				return
			}
		case "Interval":
			z.Interval, bts, err = msgp.ReadIntBytes(bts)
			if err != nil {
				return
			}
		case "Unit":
			z.Unit, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		case "tagkeyvalues":
			bts, err = msgp.ReadExtensionBytes(bts, &z.Tags)
			if err != nil {
				return
			}
		case "LastUpdate":
			z.LastUpdate, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				return
			}
		case "Partition":
			z.Partition, bts, err = msgp.ReadInt32Bytes(bts)
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

func (z *MetricDefinition) Msgsize() (s int) {
	s = 1 + 3 + z.Id.Msgsize() + 6 + msgp.Uint32Size + 5 + msgp.ExtensionPrefixSize + z.Name.Len() + 9 + msgp.IntSize + 5 + msgp.StringPrefixSize + len(z.Unit) + 13 + msgp.ExtensionPrefixSize + z.Tags.Len() + 11 + msgp.Int64Size + 10 + msgp.Int32Size
	return
}

// DecodeMsg implements msgp.Decodable
func (z *TagKeyValue) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "Key":
			err = z.Key.DecodeMsg(dc)
			if err != nil {
				return
			}
		case "Value":
			err = z.Value.DecodeMsg(dc)
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
func (z *TagKeyValue) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 2
	// write "Key"
	err = en.Append(0x82, 0xa3, 0x4b, 0x65, 0x79)
	if err != nil {
		return err
	}
	err = z.Key.EncodeMsg(en)
	if err != nil {
		return
	}
	// write "Value"
	err = en.Append(0xa5, 0x56, 0x61, 0x6c, 0x75, 0x65)
	if err != nil {
		return err
	}
	err = z.Value.EncodeMsg(en)
	if err != nil {
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *TagKeyValue) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 2
	// string "Key"
	o = append(o, 0x82, 0xa3, 0x4b, 0x65, 0x79)
	o, err = z.Key.MarshalMsg(o)
	if err != nil {
		return
	}
	// string "Value"
	o = append(o, 0xa5, 0x56, 0x61, 0x6c, 0x75, 0x65)
	o, err = z.Value.MarshalMsg(o)
	if err != nil {
		return
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *TagKeyValue) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "Key":
			bts, err = z.Key.UnmarshalMsg(bts)
			if err != nil {
				return
			}
		case "Value":
			bts, err = z.Value.UnmarshalMsg(bts)
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

func (z *TagKeyValue) Msgsize() (s int) {
	s = 1 + 4 + z.Key.Msgsize() + 6 + z.Value.Msgsize()
	return
}

// DecodeMsg implements msgp.Decodable
func (z *TagKeyValues) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "KeyValues":
			var xsz uint32
			xsz, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.KeyValues) >= int(xsz) {
				z.KeyValues = z.KeyValues[:xsz]
			} else {
				z.KeyValues = make([]TagKeyValue, xsz)
			}
			for xvk := range z.KeyValues {
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
					case "Key":
						err = z.KeyValues[xvk].Key.DecodeMsg(dc)
						if err != nil {
							return
						}
					case "Value":
						err = z.KeyValues[xvk].Value.DecodeMsg(dc)
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
func (z *TagKeyValues) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 1
	// write "KeyValues"
	err = en.Append(0x81, 0xa9, 0x4b, 0x65, 0x79, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x73)
	if err != nil {
		return err
	}
	err = en.WriteArrayHeader(uint32(len(z.KeyValues)))
	if err != nil {
		return
	}
	for xvk := range z.KeyValues {
		// map header, size 2
		// write "Key"
		err = en.Append(0x82, 0xa3, 0x4b, 0x65, 0x79)
		if err != nil {
			return err
		}
		err = z.KeyValues[xvk].Key.EncodeMsg(en)
		if err != nil {
			return
		}
		// write "Value"
		err = en.Append(0xa5, 0x56, 0x61, 0x6c, 0x75, 0x65)
		if err != nil {
			return err
		}
		err = z.KeyValues[xvk].Value.EncodeMsg(en)
		if err != nil {
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *TagKeyValues) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 1
	// string "KeyValues"
	o = append(o, 0x81, 0xa9, 0x4b, 0x65, 0x79, 0x56, 0x61, 0x6c, 0x75, 0x65, 0x73)
	o = msgp.AppendArrayHeader(o, uint32(len(z.KeyValues)))
	for xvk := range z.KeyValues {
		// map header, size 2
		// string "Key"
		o = append(o, 0x82, 0xa3, 0x4b, 0x65, 0x79)
		o, err = z.KeyValues[xvk].Key.MarshalMsg(o)
		if err != nil {
			return
		}
		// string "Value"
		o = append(o, 0xa5, 0x56, 0x61, 0x6c, 0x75, 0x65)
		o, err = z.KeyValues[xvk].Value.MarshalMsg(o)
		if err != nil {
			return
		}
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *TagKeyValues) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "KeyValues":
			var xsz uint32
			xsz, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				return
			}
			if cap(z.KeyValues) >= int(xsz) {
				z.KeyValues = z.KeyValues[:xsz]
			} else {
				z.KeyValues = make([]TagKeyValue, xsz)
			}
			for xvk := range z.KeyValues {
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
					case "Key":
						bts, err = z.KeyValues[xvk].Key.UnmarshalMsg(bts)
						if err != nil {
							return
						}
					case "Value":
						bts, err = z.KeyValues[xvk].Value.UnmarshalMsg(bts)
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

func (z *TagKeyValues) Msgsize() (s int) {
	s = 1 + 10 + msgp.ArrayHeaderSize
	for xvk := range z.KeyValues {
		s += 1 + 4 + z.KeyValues[xvk].Key.Msgsize() + 6 + z.KeyValues[xvk].Value.Msgsize()
	}
	return
}
