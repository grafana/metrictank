package stats

import (
	"io"
	"net"
	"sync"
	"time"

	"github.com/grafana/metrictank/clock"
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
	// WriteGraphiteLine appends the Graphite formatted metric measurement to `buf` and resets measurements for the next interval if needed
	// `buf` is the incoming buffer to be appended to
	// `prefix` is an optional prefix to the metric name which must have a trailing '.' if present
	// `now` is the time that the metrics should be reported at
	WriteGraphiteLine(buf, prefix []byte, now time.Time) []byte
}

type Graphite struct {
	prefix []byte
	addr   string

	timeout    time.Duration
	toGraphite chan []byte
}

// NewGraphite creates and starts a graphite reporter which.
// prefix is a string prefix which is added to every metric
// addr is the graphite address to report to
// interval is the interval in seconds that metrics should be reported.  If interval is negative, metrics will not be reported automatically.
// bufferSize determines how many reporting intervals should be buffered in memory.  If full, new intervals will not be reported
// timeout determines how long to wait while reporting an interval
// returns a new graphite instance which should only be used for manual reporting if interval is < 0
func NewGraphite(prefix, addr string, interval, bufferSize int, timeout time.Duration) *Graphite {
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
	if interval > 0 {
		go g.reporter(interval)
	}
	return g
}

// Report sends graphite metrics with the given timestamp.
// This should only be used if a negative reporting interval has been set.
func (g *Graphite) Report(now time.Time) {
	log.Debugf("stats flushing for %s to graphite", now)
	queueItems.Value(len(g.toGraphite))
	if cap(g.toGraphite) != 0 && len(g.toGraphite) == cap(g.toGraphite) {
		// no space in buffer, no use in doing any work
		return
	}

	pre := time.Now()

	buf := make([]byte, 0)

	for _, metric := range registry.list() {
		buf = metric.WriteGraphiteLine(buf, g.prefix, now)
	}

	genDataDuration.Set(int(time.Since(pre).Nanoseconds()))
	messageSize.Set(len(buf))
	g.toGraphite <- buf
	queueItems.Value(len(g.toGraphite))
}

func (g *Graphite) reporter(interval int) {
	ticker := clock.AlignedTickLossy(time.Duration(interval) * time.Second)
	for now := range ticker {
		g.Report(now)
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
