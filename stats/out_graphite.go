package stats

import (
	"bytes"
	"net"
	"sync"
	"time"

	"github.com/raintank/worldping-api/pkg/log"
)

type GraphiteMetric interface {
	// Report the measurements in graphite format and reset measurements for the next interval if needed
	ReportGraphite(prefix []byte, buf []byte, now time.Time) []byte
}

type Graphite struct {
	prefix                   []byte
	addr                     string
	prefixCounter32          string
	prefixGauge32            string
	prefixGauge64            string
	prefixRange32            string
	prefixLatencyHistogram32 string

	toGraphite chan []byte
}

func NewGraphite(prefix, addr string, interval int, bufferSize int) {
	if len(prefix) != 0 && prefix[len(prefix)-1] != '.' {
		prefix = prefix + "."
	}
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
		if cap(g.toGraphite) != 0 && len(g.toGraphite) == cap(g.toGraphite) {
			// no space in buffer, no use in doing any work
			continue
		}

		buf := make([]byte, 0)

		var fullPrefix bytes.Buffer
		for name, metric := range registry.list() {
			fullPrefix.Reset()
			fullPrefix.Write(g.prefix)
			fullPrefix.WriteString(name)
			fullPrefix.WriteRune('.')
			buf = metric.ReportGraphite(fullPrefix.Bytes(), buf, now)
		}
		g.toGraphite <- buf
	}
}

// graphiteWriter is the background workers that connects to graphite and submits all pending data to it
// TODO: conn.Write() returns no error for a while when the remote endpoint is down, the reconnect happens with a delay
func (g *Graphite) writer() {
	connectTicker := time.Tick(time.Second)

	lock := &sync.Mutex{}
	var conn net.Conn

	var err error
	go func() {
		for range connectTicker {
			lock.Lock()
			if conn == nil {
				conn, err = net.Dial("tcp", g.addr)
				if err == nil {
					log.Info("stats now connected to %s", g.addr)
				} else {
					log.Warn("stats dialing %s failed: %s. will retry", g.addr, err.Error())
				}
			}
			lock.Unlock()
		}
	}()
	for buf := range g.toGraphite {
		var ok bool
		for !ok {
			for {
				lock.Lock()
				haveConn := (conn != nil)
				lock.Unlock()
				if haveConn {
					break
				}
				time.Sleep(time.Second)
			}
			pre := time.Now()
			lock.Lock()
			_, err = conn.Write(buf)
			if err == nil {
				ok = true
			} else {
				log.Warn("stats failed to write to graphite: %s (took %s). will retry...", err, time.Now().Sub(pre))
				conn.Close()
				conn = nil
			}
			lock.Unlock()
		}
	}
	lock.Lock()
	if conn != nil {
		conn.Close()
	}
	lock.Unlock()
}
