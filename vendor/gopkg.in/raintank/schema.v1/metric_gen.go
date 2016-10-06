package schema

// NOTE: THIS FILE WAS PRODUCED BY THE
// MSGP CODE GENERATION TOOL (github.com/tinylib/msgp)
// DO NOT EDIT

import "github.com/tinylib/msgp/msgp"

// DecodeMsg implements msgp.Decodable
func (z *MetricData) DecodeMsg(dc *msgp.Reader) (err error) {
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
		case "Id":
			z.Id, err = dc.ReadString()
			if err != nil {
				return
			}
		case "OrgId":
			z.OrgId, err = dc.ReadInt()
			if err != nil {
				return
			}
		case "Name":
			z.Name, err = dc.ReadString()
			if err != nil {
				return
			}
		case "Metric":
			z.Metric, err = dc.ReadString()
			if err != nil {
				return
			}
		case "Interval":
			z.Interval, err = dc.ReadInt()
			if err != nil {
				return
			}
		case "Value":
			z.Value, err = dc.ReadFloat64()
			if err != nil {
				return
			}
		case "Unit":
			z.Unit, err = dc.ReadString()
			if err != nil {
				return
			}
		case "Time":
			z.Time, err = dc.ReadInt64()
			if err != nil {
				return
			}
		case "Mtype":
			z.Mtype, err = dc.ReadString()
			if err != nil {
				return
			}
		case "Tags":
			var zbai uint32
			zbai, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.Tags) >= int(zbai) {
				z.Tags = (z.Tags)[:zbai]
			} else {
				z.Tags = make([]string, zbai)
			}
			for zxvk := range z.Tags {
				z.Tags[zxvk], err = dc.ReadString()
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
func (z *MetricData) EncodeMsg(en *msgp.Writer) (err error) {
	// map header, size 10
	// write "Id"
	err = en.Append(0x8a, 0xa2, 0x49, 0x64)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Id)
	if err != nil {
		return
	}
	// write "OrgId"
	err = en.Append(0xa5, 0x4f, 0x72, 0x67, 0x49, 0x64)
	if err != nil {
		return err
	}
	err = en.WriteInt(z.OrgId)
	if err != nil {
		return
	}
	// write "Name"
	err = en.Append(0xa4, 0x4e, 0x61, 0x6d, 0x65)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Name)
	if err != nil {
		return
	}
	// write "Metric"
	err = en.Append(0xa6, 0x4d, 0x65, 0x74, 0x72, 0x69, 0x63)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Metric)
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
	// write "Value"
	err = en.Append(0xa5, 0x56, 0x61, 0x6c, 0x75, 0x65)
	if err != nil {
		return err
	}
	err = en.WriteFloat64(z.Value)
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
	// write "Time"
	err = en.Append(0xa4, 0x54, 0x69, 0x6d, 0x65)
	if err != nil {
		return err
	}
	err = en.WriteInt64(z.Time)
	if err != nil {
		return
	}
	// write "Mtype"
	err = en.Append(0xa5, 0x4d, 0x74, 0x79, 0x70, 0x65)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Mtype)
	if err != nil {
		return
	}
	// write "Tags"
	err = en.Append(0xa4, 0x54, 0x61, 0x67, 0x73)
	if err != nil {
		return err
	}
	err = en.WriteArrayHeader(uint32(len(z.Tags)))
	if err != nil {
		return
	}
	for zxvk := range z.Tags {
		err = en.WriteString(z.Tags[zxvk])
		if err != nil {
			return
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *MetricData) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 10
	// string "Id"
	o = append(o, 0x8a, 0xa2, 0x49, 0x64)
	o = msgp.AppendString(o, z.Id)
	// string "OrgId"
	o = append(o, 0xa5, 0x4f, 0x72, 0x67, 0x49, 0x64)
	o = msgp.AppendInt(o, z.OrgId)
	// string "Name"
	o = append(o, 0xa4, 0x4e, 0x61, 0x6d, 0x65)
	o = msgp.AppendString(o, z.Name)
	// string "Metric"
	o = append(o, 0xa6, 0x4d, 0x65, 0x74, 0x72, 0x69, 0x63)
	o = msgp.AppendString(o, z.Metric)
	// string "Interval"
	o = append(o, 0xa8, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x76, 0x61, 0x6c)
	o = msgp.AppendInt(o, z.Interval)
	// string "Value"
	o = append(o, 0xa5, 0x56, 0x61, 0x6c, 0x75, 0x65)
	o = msgp.AppendFloat64(o, z.Value)
	// string "Unit"
	o = append(o, 0xa4, 0x55, 0x6e, 0x69, 0x74)
	o = msgp.AppendString(o, z.Unit)
	// string "Time"
	o = append(o, 0xa4, 0x54, 0x69, 0x6d, 0x65)
	o = msgp.AppendInt64(o, z.Time)
	// string "Mtype"
	o = append(o, 0xa5, 0x4d, 0x74, 0x79, 0x70, 0x65)
	o = msgp.AppendString(o, z.Mtype)
	// string "Tags"
	o = append(o, 0xa4, 0x54, 0x61, 0x67, 0x73)
	o = msgp.AppendArrayHeader(o, uint32(len(z.Tags)))
	for zxvk := range z.Tags {
		o = msgp.AppendString(o, z.Tags[zxvk])
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *MetricData) UnmarshalMsg(bts []byte) (o []byte, err error) {
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
		case "Id":
			z.Id, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		case "OrgId":
			z.OrgId, bts, err = msgp.ReadIntBytes(bts)
			if err != nil {
				return
			}
		case "Name":
			z.Name, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		case "Metric":
			z.Metric, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		case "Interval":
			z.Interval, bts, err = msgp.ReadIntBytes(bts)
			if err != nil {
				return
			}
		case "Value":
			z.Value, bts, err = msgp.ReadFloat64Bytes(bts)
			if err != nil {
				return
			}
		case "Unit":
			z.Unit, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		case "Time":
			z.Time, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				return
			}
		case "Mtype":
			z.Mtype, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		case "Tags":
			var zajw uint32
			zajw, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				return
			}
			if cap(z.Tags) >= int(zajw) {
				z.Tags = (z.Tags)[:zajw]
			} else {
				z.Tags = make([]string, zajw)
			}
			for zxvk := range z.Tags {
				z.Tags[zxvk], bts, err = msgp.ReadStringBytes(bts)
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
func (z *MetricData) Msgsize() (s int) {
	s = 1 + 3 + msgp.StringPrefixSize + len(z.Id) + 6 + msgp.IntSize + 5 + msgp.StringPrefixSize + len(z.Name) + 7 + msgp.StringPrefixSize + len(z.Metric) + 9 + msgp.IntSize + 6 + msgp.Float64Size + 5 + msgp.StringPrefixSize + len(z.Unit) + 5 + msgp.Int64Size + 6 + msgp.StringPrefixSize + len(z.Mtype) + 5 + msgp.ArrayHeaderSize
	for zxvk := range z.Tags {
		s += msgp.StringPrefixSize + len(z.Tags[zxvk])
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *MetricDataArray) DecodeMsg(dc *msgp.Reader) (err error) {
	var zcua uint32
	zcua, err = dc.ReadArrayHeader()
	if err != nil {
		return
	}
	if cap((*z)) >= int(zcua) {
		(*z) = (*z)[:zcua]
	} else {
		(*z) = make(MetricDataArray, zcua)
	}
	for zhct := range *z {
		if dc.IsNil() {
			err = dc.ReadNil()
			if err != nil {
				return
			}
			(*z)[zhct] = nil
		} else {
			if (*z)[zhct] == nil {
				(*z)[zhct] = new(MetricData)
			}
			err = (*z)[zhct].DecodeMsg(dc)
			if err != nil {
				return
			}
		}
	}
	return
}

// EncodeMsg implements msgp.Encodable
func (z MetricDataArray) EncodeMsg(en *msgp.Writer) (err error) {
	err = en.WriteArrayHeader(uint32(len(z)))
	if err != nil {
		return
	}
	for zxhx := range z {
		if z[zxhx] == nil {
			err = en.WriteNil()
			if err != nil {
				return
			}
		} else {
			err = z[zxhx].EncodeMsg(en)
			if err != nil {
				return
			}
		}
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z MetricDataArray) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	o = msgp.AppendArrayHeader(o, uint32(len(z)))
	for zxhx := range z {
		if z[zxhx] == nil {
			o = msgp.AppendNil(o)
		} else {
			o, err = z[zxhx].MarshalMsg(o)
			if err != nil {
				return
			}
		}
	}
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *MetricDataArray) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var zdaf uint32
	zdaf, bts, err = msgp.ReadArrayHeaderBytes(bts)
	if err != nil {
		return
	}
	if cap((*z)) >= int(zdaf) {
		(*z) = (*z)[:zdaf]
	} else {
		(*z) = make(MetricDataArray, zdaf)
	}
	for zlqf := range *z {
		if msgp.IsNil(bts) {
			bts, err = msgp.ReadNilBytes(bts)
			if err != nil {
				return
			}
			(*z)[zlqf] = nil
		} else {
			if (*z)[zlqf] == nil {
				(*z)[zlqf] = new(MetricData)
			}
			bts, err = (*z)[zlqf].UnmarshalMsg(bts)
			if err != nil {
				return
			}
		}
	}
	o = bts
	return
}

// Msgsize returns an upper bound estimate of the number of bytes occupied by the serialized message
func (z MetricDataArray) Msgsize() (s int) {
	s = msgp.ArrayHeaderSize
	for zpks := range z {
		if z[zpks] == nil {
			s += msgp.NilSize
		} else {
			s += z[zpks].Msgsize()
		}
	}
	return
}

// DecodeMsg implements msgp.Decodable
func (z *MetricDefinition) DecodeMsg(dc *msgp.Reader) (err error) {
	var field []byte
	_ = field
	var zrsw uint32
	zrsw, err = dc.ReadMapHeader()
	if err != nil {
		return
	}
	for zrsw > 0 {
		zrsw--
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
		case "OrgId":
			z.OrgId, err = dc.ReadInt()
			if err != nil {
				return
			}
		case "Name":
			z.Name, err = dc.ReadString()
			if err != nil {
				return
			}
		case "Metric":
			z.Metric, err = dc.ReadString()
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
		case "Mtype":
			z.Mtype, err = dc.ReadString()
			if err != nil {
				return
			}
		case "Tags":
			var zxpk uint32
			zxpk, err = dc.ReadArrayHeader()
			if err != nil {
				return
			}
			if cap(z.Tags) >= int(zxpk) {
				z.Tags = (z.Tags)[:zxpk]
			} else {
				z.Tags = make([]string, zxpk)
			}
			for zjfb := range z.Tags {
				z.Tags[zjfb], err = dc.ReadString()
				if err != nil {
					return
				}
			}
		case "LastUpdate":
			z.LastUpdate, err = dc.ReadInt64()
			if err != nil {
				return
			}
		case "Nodes":
			var zdnj uint32
			zdnj, err = dc.ReadMapHeader()
			if err != nil {
				return
			}
			if z.Nodes == nil && zdnj > 0 {
				z.Nodes = make(map[string]string, zdnj)
			} else if len(z.Nodes) > 0 {
				for key, _ := range z.Nodes {
					delete(z.Nodes, key)
				}
			}
			for zdnj > 0 {
				zdnj--
				var zcxo string
				var zeff string
				zcxo, err = dc.ReadString()
				if err != nil {
					return
				}
				zeff, err = dc.ReadString()
				if err != nil {
					return
				}
				z.Nodes[zcxo] = zeff
			}
		case "NodeCount":
			z.NodeCount, err = dc.ReadInt()
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
	// map header, size 11
	// write "Id"
	err = en.Append(0x8b, 0xa2, 0x49, 0x64)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Id)
	if err != nil {
		return
	}
	// write "OrgId"
	err = en.Append(0xa5, 0x4f, 0x72, 0x67, 0x49, 0x64)
	if err != nil {
		return err
	}
	err = en.WriteInt(z.OrgId)
	if err != nil {
		return
	}
	// write "Name"
	err = en.Append(0xa4, 0x4e, 0x61, 0x6d, 0x65)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Name)
	if err != nil {
		return
	}
	// write "Metric"
	err = en.Append(0xa6, 0x4d, 0x65, 0x74, 0x72, 0x69, 0x63)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Metric)
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
	// write "Mtype"
	err = en.Append(0xa5, 0x4d, 0x74, 0x79, 0x70, 0x65)
	if err != nil {
		return err
	}
	err = en.WriteString(z.Mtype)
	if err != nil {
		return
	}
	// write "Tags"
	err = en.Append(0xa4, 0x54, 0x61, 0x67, 0x73)
	if err != nil {
		return err
	}
	err = en.WriteArrayHeader(uint32(len(z.Tags)))
	if err != nil {
		return
	}
	for zjfb := range z.Tags {
		err = en.WriteString(z.Tags[zjfb])
		if err != nil {
			return
		}
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
	// write "Nodes"
	err = en.Append(0xa5, 0x4e, 0x6f, 0x64, 0x65, 0x73)
	if err != nil {
		return err
	}
	err = en.WriteMapHeader(uint32(len(z.Nodes)))
	if err != nil {
		return
	}
	for zcxo, zeff := range z.Nodes {
		err = en.WriteString(zcxo)
		if err != nil {
			return
		}
		err = en.WriteString(zeff)
		if err != nil {
			return
		}
	}
	// write "NodeCount"
	err = en.Append(0xa9, 0x4e, 0x6f, 0x64, 0x65, 0x43, 0x6f, 0x75, 0x6e, 0x74)
	if err != nil {
		return err
	}
	err = en.WriteInt(z.NodeCount)
	if err != nil {
		return
	}
	return
}

// MarshalMsg implements msgp.Marshaler
func (z *MetricDefinition) MarshalMsg(b []byte) (o []byte, err error) {
	o = msgp.Require(b, z.Msgsize())
	// map header, size 11
	// string "Id"
	o = append(o, 0x8b, 0xa2, 0x49, 0x64)
	o = msgp.AppendString(o, z.Id)
	// string "OrgId"
	o = append(o, 0xa5, 0x4f, 0x72, 0x67, 0x49, 0x64)
	o = msgp.AppendInt(o, z.OrgId)
	// string "Name"
	o = append(o, 0xa4, 0x4e, 0x61, 0x6d, 0x65)
	o = msgp.AppendString(o, z.Name)
	// string "Metric"
	o = append(o, 0xa6, 0x4d, 0x65, 0x74, 0x72, 0x69, 0x63)
	o = msgp.AppendString(o, z.Metric)
	// string "Interval"
	o = append(o, 0xa8, 0x49, 0x6e, 0x74, 0x65, 0x72, 0x76, 0x61, 0x6c)
	o = msgp.AppendInt(o, z.Interval)
	// string "Unit"
	o = append(o, 0xa4, 0x55, 0x6e, 0x69, 0x74)
	o = msgp.AppendString(o, z.Unit)
	// string "Mtype"
	o = append(o, 0xa5, 0x4d, 0x74, 0x79, 0x70, 0x65)
	o = msgp.AppendString(o, z.Mtype)
	// string "Tags"
	o = append(o, 0xa4, 0x54, 0x61, 0x67, 0x73)
	o = msgp.AppendArrayHeader(o, uint32(len(z.Tags)))
	for zjfb := range z.Tags {
		o = msgp.AppendString(o, z.Tags[zjfb])
	}
	// string "LastUpdate"
	o = append(o, 0xaa, 0x4c, 0x61, 0x73, 0x74, 0x55, 0x70, 0x64, 0x61, 0x74, 0x65)
	o = msgp.AppendInt64(o, z.LastUpdate)
	// string "Nodes"
	o = append(o, 0xa5, 0x4e, 0x6f, 0x64, 0x65, 0x73)
	o = msgp.AppendMapHeader(o, uint32(len(z.Nodes)))
	for zcxo, zeff := range z.Nodes {
		o = msgp.AppendString(o, zcxo)
		o = msgp.AppendString(o, zeff)
	}
	// string "NodeCount"
	o = append(o, 0xa9, 0x4e, 0x6f, 0x64, 0x65, 0x43, 0x6f, 0x75, 0x6e, 0x74)
	o = msgp.AppendInt(o, z.NodeCount)
	return
}

// UnmarshalMsg implements msgp.Unmarshaler
func (z *MetricDefinition) UnmarshalMsg(bts []byte) (o []byte, err error) {
	var field []byte
	_ = field
	var zobc uint32
	zobc, bts, err = msgp.ReadMapHeaderBytes(bts)
	if err != nil {
		return
	}
	for zobc > 0 {
		zobc--
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
		case "OrgId":
			z.OrgId, bts, err = msgp.ReadIntBytes(bts)
			if err != nil {
				return
			}
		case "Name":
			z.Name, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		case "Metric":
			z.Metric, bts, err = msgp.ReadStringBytes(bts)
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
		case "Mtype":
			z.Mtype, bts, err = msgp.ReadStringBytes(bts)
			if err != nil {
				return
			}
		case "Tags":
			var zsnv uint32
			zsnv, bts, err = msgp.ReadArrayHeaderBytes(bts)
			if err != nil {
				return
			}
			if cap(z.Tags) >= int(zsnv) {
				z.Tags = (z.Tags)[:zsnv]
			} else {
				z.Tags = make([]string, zsnv)
			}
			for zjfb := range z.Tags {
				z.Tags[zjfb], bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					return
				}
			}
		case "LastUpdate":
			z.LastUpdate, bts, err = msgp.ReadInt64Bytes(bts)
			if err != nil {
				return
			}
		case "Nodes":
			var zkgt uint32
			zkgt, bts, err = msgp.ReadMapHeaderBytes(bts)
			if err != nil {
				return
			}
			if z.Nodes == nil && zkgt > 0 {
				z.Nodes = make(map[string]string, zkgt)
			} else if len(z.Nodes) > 0 {
				for key, _ := range z.Nodes {
					delete(z.Nodes, key)
				}
			}
			for zkgt > 0 {
				var zcxo string
				var zeff string
				zkgt--
				zcxo, bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					return
				}
				zeff, bts, err = msgp.ReadStringBytes(bts)
				if err != nil {
					return
				}
				z.Nodes[zcxo] = zeff
			}
		case "NodeCount":
			z.NodeCount, bts, err = msgp.ReadIntBytes(bts)
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
func (z *MetricDefinition) Msgsize() (s int) {
	s = 1 + 3 + msgp.StringPrefixSize + len(z.Id) + 6 + msgp.IntSize + 5 + msgp.StringPrefixSize + len(z.Name) + 7 + msgp.StringPrefixSize + len(z.Metric) + 9 + msgp.IntSize + 5 + msgp.StringPrefixSize + len(z.Unit) + 6 + msgp.StringPrefixSize + len(z.Mtype) + 5 + msgp.ArrayHeaderSize
	for zjfb := range z.Tags {
		s += msgp.StringPrefixSize + len(z.Tags[zjfb])
	}
	s += 11 + msgp.Int64Size + 6 + msgp.MapHeaderSize
	if z.Nodes != nil {
		for zcxo, zeff := range z.Nodes {
			_ = zeff
			s += msgp.StringPrefixSize + len(zcxo) + msgp.StringPrefixSize + len(zeff)
		}
	}
	s += 10 + msgp.IntSize
	return
}
