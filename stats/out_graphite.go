package stats

import (
	"bytes"
	"io"
	"net"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
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

	timeout    time.Duration
	toGraphite chan []byte
}

func NewGraphite(prefix, addr string, interval, bufferSize int, timeout time.Duration) {
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
		timeout:    timeout,
	}
	go g.writer()
	go g.reporter(interval)
}

func (g *Graphite) reporter(interval int) {
	ticker := tick(time.Duration(interval) * time.Second)
	for now := range ticker {
		log.Debugf("stats flushing for %s to graphite", now)
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
func (g *Graphite) writer() {
	var conn net.Conn
	var err error
	var wg sync.WaitGroup

	assureConn := func() {
		connected.Set(conn != nil)
		for conn == nil {
			time.Sleep(time.Second)
			conn, err = net.Dial("tcp", g.addr)
			if err == nil {
				log.Infof("stats now connected to %s", g.addr)
				wg.Add(1)
				go g.checkEOF(conn, &wg)
			} else {
				log.Warnf("stats dialing %s failed: %s. will retry", g.addr, err.Error())
			}
			connected.Set(conn != nil)
		}
	}

	for buf := range g.toGraphite {
		queueItems.Value(len(g.toGraphite))
		var ok bool
		for !ok {
			assureConn()
			conn.SetWriteDeadline(time.Now().Add(g.timeout))
			pre := time.Now()
			_, err = conn.Write(buf)
			if err == nil {
				ok = true
				flushDuration.Value(time.Since(pre))
			} else {
				log.Warnf("stats failed to write to graphite: %s (took %s). will retry...", err, time.Now().Sub(pre))
				conn.Close()
				wg.Wait()
				conn = nil
			}
		}
	}
}

// normally the remote end should never write anything back
// but we know when we get EOF that the other end closed the conn
// if not for this, we can happily write and flush without getting errors (in Go) but getting RST tcp packets back (!)
// props to Tv` for this trick.
func (g *Graphite) checkEOF(conn net.Conn, wg *sync.WaitGroup) {
	defer wg.Done()
	b := make([]byte, 1024)
	for {
		num, err := conn.Read(b)
		if err == io.EOF {
			log.Info("Graphite.checkEOF: remote closed conn. closing conn")
			conn.Close()
			return
		}

		// in case the remote behaves badly (out of spec for carbon protocol)
		if num != 0 {
			log.Warnf("Graphite.checkEOF: read unexpected data from peer: %s\n", b[:num])
			continue
		}

		if err != io.EOF {
			log.Warnf("Graphite.checkEOF: %s. closing conn\n", err)
			conn.Close()
			return
		}
	}
}
