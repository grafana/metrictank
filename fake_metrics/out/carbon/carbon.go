package carbon

import (
	"bytes"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/raintank/met"
	"github.com/raintank/raintank-metric/schema"
)

var (
	metricsPublished met.Count
	publishDuration  met.Timer
)

type Carbon struct {
	sync.Mutex
	addr string
	conn net.Conn
}

func New(addr string, stats met.Backend) (*Carbon, error) {

	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	metricsPublished = stats.NewCount("metricpublisher.carbon.metrics-published")
	publishDuration = stats.NewTimer("metricpublisher.carbon.publish_duration", 0)

	return &Carbon{
		sync.Mutex{},
		addr,
		conn,
	}, nil
}

func (n *Carbon) Close() error {
	return n.conn.Close()
}

func (n *Carbon) Publish(metrics []*schema.MetricData) error {
	if len(metrics) == 0 {
		return nil
	}
	pre := time.Now()
	buf := bytes.NewBufferString("")
	for _, m := range metrics {
		buf.WriteString(fmt.Sprintf("org_%d.%s %f %d\n", m.OrgId, m.Name, m.Value, m.Time))
	}
	n.Lock()
	_, err := n.conn.Write(buf.Bytes())
	n.Unlock()
	if err != nil {
		return err
	}
	publishDuration.Value(time.Since(pre))
	metricsPublished.Inc(int64(len(metrics)))
	return nil
}
