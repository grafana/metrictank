package carbon

import (
	"bytes"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/raintank/met"
	"github.com/raintank/raintank-metric/fake_metrics/out"
	"github.com/raintank/schema"
)

type Carbon struct {
	sync.Mutex
	out.OutStats
	addr string
	conn net.Conn
}

func New(addr string, stats met.Backend) (*Carbon, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, err
	}

	return &Carbon{
		sync.Mutex{},
		out.NewStats(stats, "carbon"),
		addr,
		conn,
	}, nil
}

func (n *Carbon) Close() error {
	return n.conn.Close()
}

func (n *Carbon) Flush(metrics []*schema.MetricData) error {
	if len(metrics) == 0 {
		n.FlushDuration.Value(0)
		return nil
	}
	preFlush := time.Now()
	buf := bytes.NewBufferString("")
	for _, m := range metrics {
		buf.WriteString(fmt.Sprintf("%s %f %d\n", m.Name, m.Value, m.Time))
	}
	prePub := time.Now()
	n.Lock()
	_, err := n.conn.Write(buf.Bytes())
	n.Unlock()
	if err != nil {
		n.PublishErrors.Inc(1)
		return err
	}
	n.MessageBytes.Value(int64(buf.Len()))
	n.MessageMetrics.Value(int64(len(metrics)))
	n.PublishedMetrics.Inc(int64(len(metrics)))
	n.PublishedMessages.Inc(1)
	n.PublishDuration.Value(time.Since(prePub))
	n.FlushDuration.Value(time.Since(preFlush))
	return nil
}
