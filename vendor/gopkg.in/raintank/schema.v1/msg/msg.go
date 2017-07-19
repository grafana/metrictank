package msg

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/codeskyblue/go-uuid"
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

type ProbeEvent struct {
	Id       int64
	Produced time.Time
	Event    *schema.ProbeEvent
	Format   Format
	Msg      []byte
}

type ProbeEventJson struct {
	Id        string   `json:"id"`
	EventType string   `json:"event_type"`
	OrgId     int64    `json:"org_id"`
	Severity  string   `json:"severity"`
	Source    string   `json:"source"`
	Timestamp int64    `json:"timestamp"`
	Message   string   `json:"message"`
	Tags      []string `json:"tags"`
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

func ProbeEventFromMsg(msg []byte) (*ProbeEvent, error) {
	e := &ProbeEvent{
		Event: &schema.ProbeEvent{},
		Msg:   msg,
	}
	if len(msg) < 9 {
		return e, errTooSmall
	}

	buf := bytes.NewReader(msg[1:9])
	binary.Read(buf, binary.BigEndian, &e.Id)
	e.Produced = time.Unix(0, e.Id)

	format := Format(msg[0])
	if format != FormatProbeEventJson && format != FormatProbeEventMsgp {
		return e, fmt.Errorf(errFmtUnknownFormat, format)
	}
	e.Format = format
	return e, nil
}

func (e *ProbeEvent) DecodeProbeEvent() error {
	var err error
	switch e.Format {
	case FormatProbeEventJson:
		oldFormat := &ProbeEventJson{}
		err = json.Unmarshal(e.Msg[9:], oldFormat)
		//convert our []string of key:valy pairs to
		// map[string]string
		tags := make(map[string]string)
		for _, t := range oldFormat.Tags {
			parts := strings.SplitN(t, ":", 2)
			tags[parts[0]] = parts[1]
		}
		e.Event = &schema.ProbeEvent{
			Id:        oldFormat.Id,
			EventType: oldFormat.EventType,
			OrgId:     oldFormat.OrgId,
			Severity:  oldFormat.Severity,
			Source:    oldFormat.Source,
			Timestamp: oldFormat.Timestamp,
			Message:   oldFormat.Message,
			Tags:      tags,
		}
	case FormatProbeEventMsgp:
		_, err = e.Event.UnmarshalMsg(e.Msg[9:])
	default:
		return fmt.Errorf(errFmtUnknownFormat, e.Msg[0])
	}
	if err != nil {
		return fmt.Errorf("ERROR: failure to unmarshal message body via format %q: %s", e.Format, err)
	}
	return nil
}

func CreateProbeEventMsg(event *schema.ProbeEvent, id int64, version Format) ([]byte, error) {
	if event.Id == "" {
		// per http://blog.mikemccandless.com/2014/05/choosing-fast-unique-identifier-uuid.html,
		// using V1 UUIDs is much faster than v4 like we were using
		u := uuid.NewUUID()
		event.Id = u.String()
	}
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
	case FormatProbeEventJson:
		msg, err = json.Marshal(event)
	case FormatProbeEventMsgp:
		msg, err = event.MarshalMsg(nil)
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
