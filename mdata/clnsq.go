package mdata

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"time"

	"github.com/bitly/go-hostpool"
	"github.com/nsqio/go-nsq"
	"github.com/raintank/met"
	clNSQ "github.com/raintank/metrictank/mdata/clnsq"
	"github.com/raintank/misc/instrumented_nsq"
	"github.com/raintank/worldping-api/pkg/log"
)

var (
	hostPool    hostpool.HostPool
	producers   map[string]*nsq.Producer
	Enabled     bool
	nsqdAdds    []string
	lookupdAdds []string
	topic       string
	channel     string
	pCfg        *nsq.Config
	cCfg        *nsq.Config
)

type ClNSQ struct {
	in       chan SavedChunk
	buf      []SavedChunk
	instance string
	Cl
}

func NewNSQ(instance string, metrics Metrics, stats met.Backend) *ClNSQ {
	Enabled = clNSQ.Enabled
	nsqdAdds = clNSQ.NsqdAdds
	lookupdAdds = clNSQ.LookupdAdds
	topic = clNSQ.Topic
	channel = clNSQ.Channel
	pCfg = clNSQ.PCfg
	cCfg = clNSQ.CCfg

	// producers
	hostPool = hostpool.NewEpsilonGreedy(nsqdAdds, 0, &hostpool.LinearEpsilonValueCalculator{})
	producers = make(map[string]*nsq.Producer)

	for _, addr := range nsqdAdds {
		producer, err := nsq.NewProducer(addr, pCfg)
		if err != nil {
			log.Fatal(4, "failed creating producer %s", err.Error())
		}
		producers[addr] = producer
	}

	// consumers
	consumer, err := insq.NewConsumer(topic, channel, cCfg, "metric_persist.%s", stats)
	if err != nil {
		log.Fatal(4, "Failed to create NSQ consumer. %s", err)
	}
	c := &ClNSQ{
		in:       make(chan SavedChunk),
		instance: instance,
		Cl: Cl{
			instance: instance,
			metrics:  metrics,
		},
	}
	consumer.AddConcurrentHandlers(c, 2)

	err = consumer.ConnectToNSQDs(nsqdAdds)
	if err != nil {
		log.Fatal(4, "failed to connect to NSQDs. %s", err)
	}
	log.Info("persist consumer connected to nsqd")

	err = consumer.ConnectToNSQLookupds(lookupdAdds)
	if err != nil {
		log.Fatal(4, "failed to connect to NSQLookupds. %s", err)
	}
	go c.run()
	return c
}

func (c *ClNSQ) HandleMessage(m *nsq.Message) error {
	c.Handle(m.Body)
	return nil
}

func (c *ClNSQ) Send(sc SavedChunk) {
	c.in <- sc
}

func (c *ClNSQ) run() {
	ticker := time.NewTicker(time.Second)
	max := 5000
	for {
		select {
		case chunk := <-c.in:
			c.buf = append(c.buf, chunk)
			if len(c.buf) == max {
				c.flush()
			}
		case <-ticker.C:
			c.flush()
		}
	}
}

// flush makes sure the batch gets sent, asynchronously.
func (c *ClNSQ) flush() {
	if len(c.buf) == 0 {
		return
	}

	msg := PersistMessageBatch{Instance: c.instance, SavedChunks: c.buf}
	c.buf = nil

	go func() {
		log.Debug("CLU sending %d batch metricPersist messages", len(msg.SavedChunks))

		data, err := json.Marshal(&msg)
		if err != nil {
			log.Fatal(4, "failed to marshal persistMessage to json.")
		}
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, uint8(PersistMessageBatchV1))
		buf.Write(data)
		messagesSize.Value(int64(buf.Len()))

		sent := false
		for !sent {
			// This will always return a host. If all hosts are currently marked as dead,
			// then all hosts will be reset to alive and we will try them all again. This
			// will result in this loop repeating forever until we successfully publish our msg.
			hostPoolResponse := hostPool.Get()
			prod := producers[hostPoolResponse.Host()]
			err = prod.Publish(topic, buf.Bytes())
			// Hosts that are marked as dead will be retried after 30seconds.  If we published
			// successfully, then sending a nil error will mark the host as alive again.
			hostPoolResponse.Mark(err)
			if err != nil {
				log.Warn("publisher marking host %s as faulty due to %s", hostPoolResponse.Host(), err)
			} else {
				sent = true
			}
			time.Sleep(time.Second)
		}
		messagesPublished.Inc(1)
	}()
}
