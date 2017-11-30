package msg

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"gopkg.in/raintank/schema.v1"
)

var errTooSmall = errors.New("too small")
var errFmtBinWriteFailed = "binary write failed: %q"
var errFmtUnknownFormat = "unknown format %d"

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
		return fmt.Errorf(errFmtUnknownFormat, m.Format)
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
		return nil, fmt.Errorf(errFmtUnknownFormat, version)
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
