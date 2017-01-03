package stats

import (
	"bytes"
	"net"
	"time"

	"github.com/raintank/worldping-api/pkg/log"
)

var (
	queueItems      *Range32
	genDataDuration *Gauge32
	flushDuration   *LatencyHistogram15s32
	messageSize     *Gauge32
	connected       *Bool
)

type GraphiteMetric interface {
	// Report the measurements in graphite format and reset measurements for the next interval if needed
	ReportGraphite(prefix []byte, buf []byte, now time.Time) []byte
}

type Graphite struct {
	prefix []byte
	addr   string

	toGraphite chan []byte
}

func NewGraphite(prefix, addr string, interval int, bufferSize int) {
	if len(prefix) != 0 && prefix[len(prefix)-1] != '.' {
		prefix = prefix + "."
	}
	NewGauge32("stats.graphite.write_queue.size").Set(bufferSize)
	queueItems = NewRange32("stats.graphite.write_queue.items")
	// metric stats.generate_message is how long it takes to generate the stats
	genDataDuration = NewGauge32("stats.generate_message.duration")
	flushDuration = NewLatencyHistogram15s32("stats.graphite.flush")
	messageSize = NewGauge32("stats.message_size")
	connected = NewBool("stats.graphite.connected")

	g := &Graphite{
		prefix:     []byte(prefix),
		addr:       addr,
		toGraphite: make(chan []byte, bufferSize),
	}
	go g.writer()
	go g.reporter(interval)
}

func (g *Graphite) reporter(interval int) {
	ticker := tick(time.Duration(interval) * time.Second)
	for now := range ticker {
		log.Debug("stats flushing for", now, "to graphite")
		queueItems.Value(len(g.toGraphite))
		if cap(g.toGraphite) != 0 && len(g.toGraphite) == cap(g.toGraphite) {
			// no space in buffer, no use in doing any work
			continue
		}

		pre := time.Now()

		buf := make([]byte, 0)

		var fullPrefix bytes.Buffer
		for name, metric := range registry.list() {
			fullPrefix.Reset()
			fullPrefix.Write(g.prefix)
			fullPrefix.WriteString(name)
			fullPrefix.WriteRune('.')
			buf = metric.ReportGraphite(fullPrefix.Bytes(), buf, now)
		}

		genDataDuration.Set(int(time.Since(pre).Nanoseconds()))
		messageSize.Set(len(buf))
		g.toGraphite <- buf
		queueItems.Value(len(g.toGraphite))
	}
}

// writer connects to graphite and submits all pending data to it
// TODO: conn.Write() returns no error for a while when the remote endpoint is down, the reconnect happens with a delay. this can also cause lost data for a second or two.
func (g *Graphite) writer() {
	var conn net.Conn
	var err error

	assureConn := func() net.Conn {
		connected.Set(conn != nil)
		for conn == nil {
			time.Sleep(time.Second)
			conn, err = net.Dial("tcp", g.addr)
			if err == nil {
				log.Info("stats now connected to %s", g.addr)
			} else {
				log.Warn("stats dialing %s failed: %s. will retry", g.addr, err.Error())
			}
			connected.Set(conn != nil)
		}
		return conn
	}

	for buf := range g.toGraphite {
		queueItems.Value(len(g.toGraphite))
		var ok bool
		for !ok {
			conn = assureConn()
			pre := time.Now()
			_, err = conn.Write(buf)
			if err == nil {
				ok = true
				flushDuration.Value(time.Since(pre))
			} else {
				log.Warn("stats failed to write to graphite: %s (took %s). will retry...", err, time.Now().Sub(pre))
				conn.Close()
				conn = nil
			}
		}
	}
}
