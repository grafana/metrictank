package msg

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/raintank/raintank-metric/schema"
)

var errMsgTooSmall = errors.New("msg too small")
var errMsgUnknownFormat = errors.New("unknown format")

type MetricData struct {
	Id       int64
	Metrics  []*schema.MetricData
	Produced time.Time
	Format   Format
	Msg      []byte
}

var mdPool = sync.Pool{
	New: func() interface{} { return make(schema.MetricDataArray, 5) }, // default size probably too small, but after some automatic reallocations should be well-tuned for real load
}

func NewMetricData(cap int) *MetricData {
	return &MetricData{
		Metrics: make([]*schema.MetricData, 0, cap),
	}
}

// parses format and id (cheap), but doesn't decode metrics (expensive) just yet.
// deprecated: this function allocates.
func MetricDataFromMsg(msg []byte) (*MetricData, error) {
	m := NewMetricData(0)
	return m, m.InitializeFromMsg(m.Msg)
}

// resets a MetricData, sets format and id (cheap), but doesn't decode metrics (expensive) just yet.
func (m *MetricData) InitializeFromMsg(msg []byte) error {
	if len(msg) < 9 {
		return errMsgTooSmall
	}
	m.Msg = msg
	buf := bytes.NewReader(m.Msg[1:9])
	binary.Read(buf, binary.BigEndian, &m.Id)
	m.Metrics = m.Metrics[:0]
	m.Produced = time.Unix(0, m.Id)

	m.Format = Format(m.Msg[0])
	if m.Format != FormatMetricDataArrayJson && m.Format != FormatMetricDataArrayMsgp {
		return errMsgUnknownFormat
	}
	return nil

}
func (m *MetricData) DecodeMetricData(buf []byte) ([]byte, error) {
	var err error
	switch m.Format {
	case FormatMetricDataArrayJson:
		err = json.Unmarshal(m.Msg[9:], &m.Metrics)
	case FormatMetricDataArrayMsgp:
		out := mdPool.Get().(schema.MetricDataArray)
		//ehh := buf[:len(m.Msg)-9]
		//fmt.Println("lenaa", len(ehh))
		//fmt.Println("capaa", cap(ehh))
		//lenaa 1341
		//capaa 10000

		// copy copies $(whichever has the largest len) bytes
		copy(buf[:len(m.Msg)-9], m.Msg[9:]) // if provided buf is large enough, we don't need to allocate.
		//copy(buf, m.Msg[9:]) // if provided buf is large enough, we don't need to allocate.
		//fmt.Println("message size", len(m.Msg)) // 1350
		//fmt.Println("message size without prefix", len(m.Msg[9:])) // 1341
		//fmt.Println("bytes copied", ret) // 1341
		//fmt.Println("buf len", len(buf))
		//fmt.Println("buf cap", cap(buf))
		//buf len 0
		//buf cap 10000

		//_, err := out.UnmarshalMsg(buf[len(m.Msg)-9:]) // slice bounds out of range
		//_, err := out.UnmarshalMsg(buf) 		// panic: msgp: too few bytes left to read object
		//_, err := out.UnmarshalMsg(buf[:0])		// panic: msgp: too few bytes left to read object
		//_, err := out.UnmarshalMsg(buf[:10000]) // works but still high alloc
		_, err := out.UnmarshalMsg(buf[:cap(buf)]) // "

		if err != nil {
			panic(err)
		}
		//fmt.Println("returned slice", len(r)) // remainder of input slice
		//message size without prefix 1381
		//bytes copied 1381
		//returned slice 8619

		m.Metrics = []*schema.MetricData(out)
		mdPool.Put(out)
	default:
		return buf, fmt.Errorf("unrecognized format %d", m.Msg[0])
	}
	if err != nil {
		return buf, fmt.Errorf("ERROR: failure to unmarshal message body via format %q: %s", m.Format, err)
	}
	return buf, nil
}

func CreateMsg(metrics []*schema.MetricData, id int64, version Format) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := binary.Write(buf, binary.LittleEndian, uint8(version))
	if err != nil {
		return nil, fmt.Errorf("binary.Write failed: %s", err.Error())
	}
	err = binary.Write(buf, binary.BigEndian, id)
	if err != nil {
		return nil, fmt.Errorf("binary.Write failed: %s", err.Error())
	}
	var msg []byte
	switch version {
	case FormatMetricDataArrayJson:
		msg, err = json.Marshal(metrics)
	case FormatMetricDataArrayMsgp:
		m := schema.MetricDataArray(metrics)
		msg, err = m.MarshalMsg(nil)
	default:
		return nil, errors.New("unsupported version")
	}
	if err != nil {
		return nil, fmt.Errorf("Failed to marshal metrics payload: %s", err)
	}
	_, err = buf.Write(msg)
	if err != nil {
		return nil, fmt.Errorf("buf.Write failed: %s", err.Error())
	}
	return buf.Bytes(), nil
}
