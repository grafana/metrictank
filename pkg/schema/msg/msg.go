package msg

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/grafana/metrictank/pkg/schema"
	"github.com/tinylib/msgp/msgp"
)

var errTooSmall = errors.New("too small")
var errFmtBinWriteFailed = "binary write failed: %q"
var errFmtUnsupportedFormat = "unsupported format %d"

type MetricData struct {
	Id       int64
	Metrics  []*schema.MetricData
	Produced time.Time
	Format   Format
	Msg      []byte
}

// parses format and id (cheap), but doesn't decode metrics (expensive) just yet.
func (m *MetricData) InitFromMsg(msg []byte) error {
	if len(msg) < 9 {
		return errTooSmall
	}
	m.Msg = msg

	buf := bytes.NewReader(msg[1:9])
	binary.Read(buf, binary.BigEndian, &m.Id)
	m.Produced = time.Unix(0, m.Id)

	m.Format = Format(msg[0])
	if m.Format != FormatMetricDataArrayJson && m.Format != FormatMetricDataArrayMsgp {
		return fmt.Errorf(errFmtUnsupportedFormat, m.Format)
	}
	return nil
}

// sets m.Metrics to a []*schema.MetricData
// any subsequent call may however put different MetricData into our m.Metrics array
func (m *MetricData) DecodeMetricData() error {
	var err error
	switch m.Format {
	case FormatMetricDataArrayJson:
		err = json.Unmarshal(m.Msg[9:], &m.Metrics)
	case FormatMetricDataArrayMsgp:
		out := schema.MetricDataArray(m.Metrics)
		_, err = out.UnmarshalMsg(m.Msg[9:])
		m.Metrics = []*schema.MetricData(out)
	default:
		return fmt.Errorf("unrecognized format %d", m.Msg[0])
	}
	if err != nil {
		return fmt.Errorf("ERROR: failure to unmarshal message body via format %q: %s", m.Format, err)
	}
	m.Msg = nil // no more need for the original input
	return nil
}

// CreateMsg is the legacy function to create messages. It's not very fast
func CreateMsg(metrics []*schema.MetricData, id int64, version Format) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, uint8(version))
	if err != nil {
		return nil, fmt.Errorf(errFmtBinWriteFailed, err)
	}
	err = binary.Write(buf, binary.BigEndian, id)
	if err != nil {
		return nil, fmt.Errorf(errFmtBinWriteFailed, err)
	}
	var msg []byte
	switch version {
	case FormatMetricDataArrayJson:
		msg, err = json.Marshal(metrics)
	case FormatMetricDataArrayMsgp:
		m := schema.MetricDataArray(metrics)
		msg, err = m.MarshalMsg(nil)
	default:
		return nil, fmt.Errorf(errFmtUnsupportedFormat, version)
	}
	if err != nil {
		return nil, fmt.Errorf("Failed to marshal metrics payload: %s", err)
	}
	_, err = buf.Write(msg)
	if err != nil {
		return nil, fmt.Errorf(errFmtBinWriteFailed, err)
	}
	return buf.Bytes(), nil
}

// WritePointMsg is like CreateMsg, except optimized for MetricPoint and buffer re-use.
// caller must assure a cap-len diff of at least:
// 33B (for FormatMetricPoint)
// 29B (for FormatMetricPointWithoutOrg)
// no other formats supported.
func WritePointMsg(point schema.MetricPoint, buf []byte, version Format) (o []byte, err error) {
	b := buf[:1]
	switch version {
	case FormatMetricPoint:
		b[0] = byte(FormatMetricPoint)
		return point.Marshal32(b)
	case FormatMetricPointWithoutOrg:
		b[0] = byte(FormatMetricPointWithoutOrg)
		return point.MarshalWithoutOrg28(b)
	}
	return nil, fmt.Errorf(errFmtUnsupportedFormat, version)
}

// WritePointMsgArray is like CreateMsg, except optimized for MetricPoint and buffer re-use.
// caller must assure a cap-len diff of exactly:
// 2 + 32B * len(points) (for FormatMetricPoint)
// 2 + 28B * len(points) (for FormatMetricPointWithoutOrg)
// no other formats supported.
func WritePointMsgArray(points []schema.MetricPoint, buf []byte, version Format) (o []byte, err error) {
	if len(points) == 0 {
		return buf, errTooSmall
	}

	b := buf[:2]
	b[0] = byte(FormatMetricPointArray)
	b[1] = byte(version)

	var formatter func(m *schema.MetricPoint, b []byte) (o []byte, err error)

	switch version {
	case FormatMetricPoint:
		formatter = (*schema.MetricPoint).Marshal32
	case FormatMetricPointWithoutOrg:
		formatter = (*schema.MetricPoint).MarshalWithoutOrg28
	default:
		return nil, fmt.Errorf(errFmtUnsupportedFormat, version)
	}

	for _, p := range points {
		b, err = formatter(&p, b)
		if err != nil {
			return b, err
		}
	}

	return b, nil
}

func IsPointMsg(data []byte) (Format, bool) {
	l := len(data)
	if l == 0 {
		return 0, false
	}
	version := Format(data[0])
	if l == 29 && version == FormatMetricPointWithoutOrg {
		return FormatMetricPointWithoutOrg, true
	}
	if l == 33 && version == FormatMetricPoint {
		return FormatMetricPoint, true
	}
	if l >= 30 && version == FormatMetricPointArray {
		subversion := Format(data[1])
		if subversion == FormatMetricPointWithoutOrg || subversion == FormatMetricPoint {
			// TODO - check length multiple?
			return FormatMetricPointArray, true
		}
	}
	return 0, false
}

func ReadPointMsg(data []byte, defaultOrg uint32) ([]byte, schema.MetricPoint, error) {
	version := Format(data[0])
	return ReadPointMsgFormat(data[1:], defaultOrg, version)
}

func ReadPointMsgFormat(data []byte, defaultOrg uint32, version Format) ([]byte, schema.MetricPoint, error) {
	var point schema.MetricPoint
	if len(data) >= 28 && version == FormatMetricPointWithoutOrg {
		o, err := point.UnmarshalWithoutOrg(data)
		point.MKey.Org = defaultOrg
		return o, point, err
	}
	if len(data) >= 32 && version == FormatMetricPoint {
		o, err := point.Unmarshal(data)
		return o, point, err
	}
	return data, point, fmt.Errorf(errFmtUnsupportedFormat, version)
}

type MetricPointIter struct {
	data       []byte
	defaultOrg uint32
	point      schema.MetricPoint
	format     Format
	err        error
}

func NewMetricPointIter(data []byte, defaultOrg uint32) MetricPointIter {
	var result MetricPointIter
	if len(data) < 30 {
		result.err = errTooSmall
		return result
	}
	version := Format(data[0])
	if version != FormatMetricPointArray {
		result.err = fmt.Errorf(errFmtUnsupportedFormat, version)
		return result
	}
	subversion := Format(data[1])
	if subversion != FormatMetricPointWithoutOrg && subversion != FormatMetricPoint {
		result.err = fmt.Errorf(errFmtUnsupportedFormat, version)
		return result
	}

	result.data = data[2:]
	result.defaultOrg = defaultOrg
	result.format = subversion

	return result
}

func (m *MetricPointIter) Next() bool {
	if len(m.data) == 0 {
		// All done
		return false
	}
	if len(m.data) < 28 {
		m.err = errTooSmall
		return false
	}
	m.data, m.point, m.err = ReadPointMsgFormat(m.data, m.defaultOrg, m.format)
	return m.err == nil
}

func (m *MetricPointIter) Value() schema.MetricPoint {
	return m.point
}

func (m *MetricPointIter) Format() Format {
	return m.format
}

func (m *MetricPointIter) Err() error {
	return m.err
}

func IsIndexControlMsg(data []byte) bool {
	l := len(data)
	if l == 0 {
		return false
	}
	version := Format(data[0])
	return version == FormatIndexControlMessage
}

func WriteIndexControlMsg(cm *schema.ControlMsg) ([]byte, error) {
	var b bytes.Buffer
	b.WriteByte(byte(FormatIndexControlMessage))
	w := msgp.NewWriterSize(&b, 300)
	err := cm.EncodeMsg(w)
	if err != nil {
		return nil, err
	}
	w.Flush()
	return b.Bytes(), nil
}

func ReadIndexControlMsg(data []byte) (schema.ControlMsg, error) {
	var control schema.ControlMsg
	_, err := control.UnmarshalMsg(data[1:])
	return control, err
}
