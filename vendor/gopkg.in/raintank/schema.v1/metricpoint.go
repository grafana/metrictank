package schema

import (
	"encoding/binary"
	"math"
)

//msgp:ignore MetricPoint

// MetricPoint is a simple way to represent a point of a metric
// it can be serialized/deserialized with and without the Org field
// of the key.
type MetricPoint struct {
	MKey  MKey
	Value float64
	Time  uint32
}

func (m *MetricPoint) Valid() bool {
	return m.Time != 0
}

// MarshalWithoutOrg28 marshals directly to b in a non-multi-tenancy aware way
// b must have a cap-len difference of at least 28 bytes.
func (m *MetricPoint) MarshalWithoutOrg28(b []byte) (o []byte, err error) {
	l := len(b)
	b = b[:l+28]
	m.putWithoutOrg(b[l:])
	return b, nil
}

// MarshalWithoutOrg marshals in a non-multi-tenancy aware way
func (m *MetricPoint) MarshalWithoutOrg(b []byte) (o []byte, err error) {
	l := len(b)
	b = ensure(b, 28) // 16+8+4
	m.putWithoutOrg(b[l:])
	return b, nil
}

func (m *MetricPoint) putWithoutOrg(b []byte) {
	copy(b, m.MKey.Key[:])
	binary.LittleEndian.PutUint64(b[16:], math.Float64bits(m.Value))
	binary.LittleEndian.PutUint32(b[24:], m.Time)
}

// UnmarshalWithoutOrg unmarshals in a non-multi-tenancy aware way
func (m *MetricPoint) UnmarshalWithoutOrg(bts []byte) (o []byte, err error) {
	copy(m.MKey.Key[:], bts[:16])
	m.Value = math.Float64frombits(binary.LittleEndian.Uint64(bts[16:24]))
	m.Time = binary.LittleEndian.Uint32(bts[24:])
	return bts[28:], nil
}

// Marshal32 marshals the MetricPoint directly to b.
// b must have a cap-len difference of at least 32 bytes.
func (m *MetricPoint) Marshal32(b []byte) (o []byte, err error) {
	l := len(b)
	b = b[:l+32]
	m.putWithoutOrg(b[l:])
	binary.LittleEndian.PutUint32(b[l+28:], m.MKey.Org)
	return b, nil
}

// Marshal marshals the MetricPoint
func (m *MetricPoint) Marshal(b []byte) (o []byte, err error) {
	l := len(b)
	b = ensure(b, 32) // 16+8+4+4
	m.putWithoutOrg(b[l:])
	binary.LittleEndian.PutUint32(b[l+28:], m.MKey.Org)
	return b, nil
}

// Unmarshal unmarshals the MetricPoint
func (m *MetricPoint) Unmarshal(bts []byte) (o []byte, err error) {
	copy(m.MKey.Key[:], bts[:16])
	m.Value = math.Float64frombits(binary.LittleEndian.Uint64(bts[16:24]))
	m.Time = binary.LittleEndian.Uint32(bts[24:])
	m.MKey.Org = binary.LittleEndian.Uint32(bts[28:])
	return bts[32:], nil
}

// ensure 'sz' extra bytes in 'b' btw len(b) and cap(b)
// and returns a slice with updated length
func ensure(b []byte, sz int) []byte {
	l := len(b)
	c := cap(b)
	if c-l < sz {
		o := make([]byte, (2*c)+sz) // exponential growth
		n := copy(o, b)
		return o[:n+sz]
	}
	return b[:l+sz]
}
