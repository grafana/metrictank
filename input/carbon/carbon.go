// package carbon provides a traditional carbon input for metrictank
// note: it does not support the "carbon2.0" protocol that serializes metrics2.0 into a plaintext carbon-like protocol
package carbon

import (
	"bufio"
	"flag"
	"io"
	"net"
	"sync"

	"github.com/lomik/go-carbon/persister"
	"github.com/metrics20/go-metrics20/carbon20"
	"github.com/raintank/met"
	"github.com/raintank/metrictank/cluster"
	"github.com/raintank/metrictank/idx"
	"github.com/raintank/metrictank/input"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/usage"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"
	"gopkg.in/raintank/schema.v1"
)

type Carbon struct {
	input.Input
	addrStr          string
	addr             *net.TCPAddr
	schemas          persister.WhisperSchemas
	stats            met.Backend
	listener         *net.TCPListener
	handlerWaitGroup sync.WaitGroup
	quit             chan struct{}
	connTrack        *ConnTrack
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
var schemasFile string
var schemas persister.WhisperSchemas
var partitionId int

func ConfigSetup() {
	inCarbon := flag.NewFlagSet("carbon-in", flag.ExitOnError)
	inCarbon.BoolVar(&Enabled, "enabled", false, "")
	inCarbon.StringVar(&addr, "addr", ":2003", "tcp listen address")
	inCarbon.IntVar(&partitionId, "partition", 1, "partition Id.")
	inCarbon.StringVar(&schemasFile, "schemas-file", "/path/to/your/schemas-file", "see http://graphite.readthedocs.io/en/latest/config-carbon.html#storage-schemas-conf")
	globalconf.Register("carbon-in", inCarbon)
}

func ConfigProcess() {
	if !Enabled {
		return
	}
	var err error
	schemas, err = persister.ReadWhisperSchemas(schemasFile)
	if err != nil {
		log.Fatal(4, "carbon-in: can't read schemas file %q: %s", schemasFile, err.Error())
	}
	var defaultFound bool
	for _, schema := range schemas {
		if schema.Pattern.String() == ".*" {
			defaultFound = true
		}
		if len(schema.Retentions) == 0 {
			log.Fatal(4, "carbon-in: retention setting cannot be empty")
		}
	}
	if !defaultFound {
		// good graphite health (not sure what graphite does if there's no .*)
		// but we definitely need to always be able to determine which interval to use
		log.Fatal(4, "carbon-in: storage-conf does not have a default '.*' pattern")
	}
	cluster.ThisNode.SetPartitions([]int32{int32(partitionId)})
}

func New(stats met.Backend) *Carbon {
	addrT, err := net.ResolveTCPAddr("tcp", addr)
	if err != nil {
		log.Fatal(4, "carbon-in: %s", err.Error())
	}
	return &Carbon{
		addrStr:   addr,
		addr:      addrT,
		schemas:   schemas,
		stats:     stats,
		quit:      make(chan struct{}),
		connTrack: NewConnTrack(),
	}
}

func (c *Carbon) Start(metrics mdata.Metrics, metricIndex idx.MetricIndex, usg *usage.Usage) {
	c.Input = input.New(metrics, metricIndex, usg, "carbon", c.stats)
	l, err := net.ListenTCP("tcp", c.addr)
	if nil != err {
		log.Fatal(4, "carbon-in: %s", err.Error())
	}
	c.listener = l
	log.Info("carbon-in: listening on %v/tcp", c.addr)
	go c.accept()
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
			log.Error(4, "carbon-in: Accept Error: %s", err.Error())
			return
		}
		c.handlerWaitGroup.Add(1)
		c.connTrack.Add(conn)
		go c.handle(conn)
	}
}

func (c *Carbon) Stop() {
	log.Info("carbon-in: shutting down.")
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
				log.Error(4, "carbon-in: Recv error: %s", err.Error())
			}
			break
		}

		key, val, ts, err := carbon20.ValidatePacket(buf, carbon20.Medium)
		if err != nil {
			c.Input.MetricsDecodeErr.Inc(1)
			log.Error(4, "carbon-in: invalid metric: %s", err.Error())
			continue
		}
		name := string(key)
		s, ok := c.schemas.Match(name)
		if !ok {
			log.Fatal(4, "carbon-in: couldn't find a schema for %q - this is impossible since we asserted there was a default with patt .*", name)
		}
		interval := s.Retentions[0].SecondsPerPoint()
		md := &schema.MetricData{
			Name:     name,
			Metric:   name,
			Interval: interval,
			Value:    val,
			Unit:     "unknown",
			Time:     int64(ts),
			Mtype:    "gauge",
			Tags:     []string{},
			OrgId:    1, // admin org
		}
		md.SetId()
		c.Input.MetricsPerMessage.Value(int64(1))
		c.Input.Process(md, int32(partitionId))
	}
	c.handlerWaitGroup.Done()
}
