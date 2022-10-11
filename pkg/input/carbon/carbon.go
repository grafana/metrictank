// package carbon provides a traditional carbon input for metrictank
// note: it does not support the "carbon2.0" protocol that serializes metrics2.0 into a plaintext carbon-like protocol
package carbon

import (
	"bufio"
	"context"
	"flag"
	"io"
	"net"
	"strings"
	"sync"

	"github.com/grafana/globalconf"
	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/input"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/stats"
	"github.com/metrics20/go-metrics20/carbon20"
	log "github.com/sirupsen/logrus"
)

// metric input.carbon.metrics_per_message is how many metrics per message were seen. in carbon's case this is always 1.
var metricsPerMessage = stats.NewMeter32("input.carbon.metrics_per_message", false)

// metric input.carbon.metrics_decode_err is a count of times an input message (MetricData, MetricDataArray or carbon line) failed to parse
var metricsDecodeErr = stats.NewCounterRate32("input.carbon.metrics_decode_err")

type Carbon struct {
	input.Handler
	addrStr          string
	addr             *net.TCPAddr
	listener         *net.TCPListener
	handlerWaitGroup sync.WaitGroup
	quit             chan struct{}
	connTrack        *ConnTrack
	intervalGetter   IntervalGetter
}

type ConnTrack struct {
	sync.Mutex
	conns map[string]net.Conn
}

func NewConnTrack() *ConnTrack {
	return &ConnTrack{
		conns: make(map[string]net.Conn),
	}
}

func (c *ConnTrack) Add(conn net.Conn) {
	c.Lock()
	c.conns[conn.RemoteAddr().String()] = conn
	c.Unlock()
}

func (c *ConnTrack) Remove(conn net.Conn) {
	c.Lock()
	delete(c.conns, conn.RemoteAddr().String())
	c.Unlock()
}

func (c *ConnTrack) CloseAll() {
	c.Lock()
	for _, conn := range c.conns {
		conn.Close()
	}
	c.Unlock()
}

func (c *Carbon) Name() string {
	return "carbon"
}

var Enabled bool
var addr string
var partitionId int

func ConfigSetup() {
	inCarbon := flag.NewFlagSet("carbon-in", flag.ExitOnError)
	inCarbon.BoolVar(&Enabled, "enabled", false, "")
	inCarbon.StringVar(&addr, "addr", ":2003", "tcp listen address")
	inCarbon.IntVar(&partitionId, "partition", 0, "partition Id.")
	globalconf.Register("carbon-in", inCarbon, flag.ExitOnError)
}

func ConfigProcess() {
	if !Enabled {
		return
	}
	cluster.Manager.SetPartitions([]int32{int32(partitionId)})
}

func New() *Carbon {
	addrT, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		log.Fatalf("carbon-in: %s", err.Error())
	}
	return &Carbon{
		addrStr:   addr,
		addr:      addrT,
		connTrack: NewConnTrack(),
	}
}

func (c *Carbon) IntervalGetter(i IntervalGetter) {
	c.intervalGetter = i
}

func (c *Carbon) Start(handler input.Handler, cancel context.CancelFunc) error {
	c.Handler = handler
	l, err := net.ListenTCP("tcp", c.addr)
	if nil != err {
		log.Errorf("carbon-in: %s", err.Error())
		return err
	}
	c.listener = l
	log.Infof("carbon-in: listening on %v/tcp", c.addr)
	c.quit = make(chan struct{})
	go c.accept()
	return nil
}

// MaintainPriority is very simplistic for carbon. there is no backfill,
// so mark as ready immediately.
func (c *Carbon) MaintainPriority() {
	cluster.Manager.SetPriority(0)
}

func (c *Carbon) ExplainPriority() interface{} {
	return "carbon-in: priority=0 (always in sync)"
}

func (c *Carbon) accept() {
	for {
		conn, err := c.listener.AcceptTCP()
		if nil != err {
			select {
			case <-c.quit:
				// we are shutting down.
				return
			default:
			}
			log.Errorf("carbon-in: Accept Error: %s", err.Error())
			return
		}
		c.handlerWaitGroup.Add(1)
		c.connTrack.Add(conn)
		go c.handle(conn)
	}
}

func (c *Carbon) Stop() {
	log.Infof("carbon-in: shutting down.")
	close(c.quit)
	c.listener.Close()
	c.connTrack.CloseAll()
	c.handlerWaitGroup.Wait()
}

func (c *Carbon) handle(conn net.Conn) {
	defer func() {
		conn.Close()
		c.connTrack.Remove(conn)
	}()
	// TODO c.SetTimeout(60e9)
	r := bufio.NewReaderSize(conn, 4096)
	for {
		// note that we don't support lines longer than 4096B. that seems very reasonable..
		buf, _, err := r.ReadLine()

		if nil != err {
			select {
			case <-c.quit:
				// we are shutting down.
				break
			default:
			}
			if io.EOF != err {
				log.Errorf("carbon-in: Recv error: %s", err.Error())
			}
			break
		}

		// no validation for m2.0 to provide a grace period in adopting new clients
		key, val, ts, err := carbon20.ValidatePacket(buf, carbon20.MediumLegacy, carbon20.NoneM20)
		if err != nil {
			metricsDecodeErr.Inc()
			log.Errorf("carbon-in: invalid metric: %s", err.Error())
			continue
		}
		nameSplits := strings.Split(string(key), ";")
		md := &schema.MetricData{
			Name:     nameSplits[0],
			Interval: c.intervalGetter.GetInterval(nameSplits[0]),
			Value:    val,
			Unit:     "unknown",
			Time:     int64(ts),
			Mtype:    "gauge",
			Tags:     nameSplits[1:],
			OrgId:    1, // admin org
		}
		md.SetId()
		metricsPerMessage.ValueUint32(1)
		c.Handler.ProcessMetricData(md, int32(partitionId))
	}
	c.handlerWaitGroup.Done()
}
