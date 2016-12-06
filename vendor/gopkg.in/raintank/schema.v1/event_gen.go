package schema

// NOTE: THIS FILE WAS PRODUCED BY THE
// MSGP CODE GENERATION TOOL (github.com/tinylib/msgp)
// DO NOT EDIT

import "github.com/tinylib/msgp/msgp"

// DecodeMsg implements msgp.Decodable
func (z *ProbeEvent) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zbai uint32
	zbai, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zbai > 0 {
		zbai--
		field, err = dc.ReadMapKeyPtr()
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "Id":
			z.Id, err = dc.ReadString()
			if err != nil {
				return
			}
		case "EventType":
			z.EventType, err = dc.ReadString()
			if err != nil {
				return
			}
		case "OrgId":
			z.OrgId, err = dc.ReadInt64()
			if err != nil {
				return
			}
		case "Severity":
			z.Severity, err = dc.ReadString()
			if err != nil {
				return
			}
		case "Source":
			z.Source, err = dc.ReadString()
			if err != nil {
				return
			}
		case "Timestamp":
			z.Timestamp, err = dc.ReadInt64()
			if err != nil {
				return
			}
		case "Message":
			z.Message, err = dc.ReadString()
			if err != nil {
				return
			}
		case "Tags":
			var zcmr uint32
			zcmr, err = dc.ReadMapHeader()
			if err != nil {
				return
			}
			if z.Tags == nil && zcmr > 0 {
				z.Tags = make(map[string]string, zcmr)
			} else if len(z.Tags) > 0 {
				for key, _ := range z.Tags {
					delete(z.Tags, key)
				}
			}
			for zcmr > 0 {
				zcmr--
				var zxvk string
				var zbzg string
				zxvk, err = dc.ReadString()
				if err != nil {
					return
				}
				zbzg, err = dc.ReadString()
				if err != nil {
					return
				}
				z.Tags[zxvk] = zbzg
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
func (z *ProbeEvent) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 8
	// write "Id"
	err = en.Append(0x88, 0xa2, 0x49, 0x64)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Id)
	if err != nil {
		return
	}
	// write "EventType"
	err = en.Append(0xa9, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x54, 0x79, 0x70, 0x65)
	if err != nil {
		return err
	}
	err = en.WriteString(z.EventType)
	if err != nil {
		return
	}
	// write "OrgId"
	err = en.Append(0xa5, 0x4f, 0x72, 0x67, 0x49, 0x64)
	if err != nil {
		return err
	}
	err = en.WriteInt64(z.OrgId)
	if err != nil {
		return
	}
	// write "Severity"
	err = en.Append(0xa8, 0x53, 0x65, 0x76, 0x65, 0x72, 0x69, 0x74, 0x79)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Severity)
	if err != nil {
		return
	}
	// write "Source"
	err = en.Append(0xa6, 0x53, 0x6f, 0x75, 0x72, 0x63, 0x65)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Source)
	if err != nil {
		return
	}
	// write "Timestamp"
	err = en.Append(0xa9, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70)
	if err != nil {
		return err
	}
	err = en.WriteInt64(z.Timestamp)
	if err != nil {
		return
	}
	// write "Message"
	err = en.Append(0xa7, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Message)
	if err != nil {
		return
	}
	// write "Tags"
	err = en.Append(0xa4, 0x54, 0x61, 0x67, 0x73)
	if err != nil {
		return err
	}
	err = en.WriteMapHeader(uint32(len(z.Tags)))
	if err != nil {
		return
	}
	for zxvk, zbzg := range z.Tags {
		err = en.WriteString(zxvk)
		if err != nil {
			return
		}
		err = en.WriteString(zbzg)
		if err != nil {
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *ProbeEvent) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 8
	// string "Id"
	o = append(o, 0x88, 0xa2, 0x49, 0x64)
	o = msgp.AppendString(o, z.Id)
	// string "EventType"
	o = append(o, 0xa9, 0x45, 0x76, 0x65, 0x6e, 0x74, 0x54, 0x79, 0x70, 0x65)
	o = msgp.AppendString(o, z.EventType)
	// string "OrgId"
	o = append(o, 0xa5, 0x4f, 0x72, 0x67, 0x49, 0x64)
	o = msgp.AppendInt64(o, z.OrgId)
	// string "Severity"
	o = append(o, 0xa8, 0x53, 0x65, 0x76, 0x65, 0x72, 0x69, 0x74, 0x79)
	o = msgp.AppendString(o, z.Severity)
	// string "Source"
	o = append(o, 0xa6, 0x53, 0x6f, 0x75, 0x72, 0x63, 0x65)
	o = msgp.AppendString(o, z.Source)
	// string "Timestamp"
	o = append(o, 0xa9, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70)
	o = msgp.AppendInt64(o, z.Timestamp)
	// string "Message"
	o = append(o, 0xa7, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65)
	o = msgp.AppendString(o, z.Message)
	// string "Tags"
	o = append(o, 0xa4, 0x54, 0x61, 0x67, 0x73)
	o = msgp.AppendMapHeader(o, uint32(len(z.Tags)))
	for zxvk, zbzg := range z.Tags {
		o = msgp.AppendString(o, zxvk)
		o = msgp.AppendString(o, zbzg)
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *ProbeEvent) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zajw uint32
	zajw, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		return
	}
	for zajw > 0 {
		zajw--
		field, bts, err = msgp.ReadMapKeyZC(bts)
		if err != nil {
			return
		}
		switch msgp.UnsafeString(field) {
		case "Id":
			z.Id, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		case "EventType":
			z.EventType, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		case "OrgId":
			z.OrgId, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				return
			}
		case "Severity":
			z.Severity, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		case "Source":
			z.Source, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		case "Timestamp":
			z.Timestamp, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				return
			}
		case "Message":
			z.Message, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		case "Tags":
			var zwht uint32
			zwht, bts, err = msgp.ReadMapHeaderBytes(bts)
			if err != nil {
				return
			}
			if z.Tags == nil && zwht > 0 {
				z.Tags = make(map[string]string, zwht)
			} else if len(z.Tags) > 0 {
				for key, _ := range z.Tags {
					delete(z.Tags, key)
				}
			}
			for zwht > 0 {
				var zxvk string
				var zbzg string
				zwht--
				zxvk, bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					return
				}
				zbzg, bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					return
				}
				z.Tags[zxvk] = zbzg
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
func (z *ProbeEvent) Msgsize() (s int) {
	s = 1 + 3 + msgp.StringPrefixSize + len(z.Id) + 10 + msgp.StringPrefixSize + len(z.EventType) + 6 + msgp.Int64Size + 9 + msgp.StringPrefixSize + len(z.Severity) + 7 + msgp.StringPrefixSize + len(z.Source) + 10 + msgp.Int64Size + 8 + msgp.StringPrefixSize + len(z.Message) + 5 + msgp.MapHeaderSize
	if z.Tags != nil {
		for zxvk, zbzg := range z.Tags {
			_ = zbzg
			s += msgp.StringPrefixSize + len(zxvk) + msgp.StringPrefixSize + len(zbzg)
		}
	}
	return
}
