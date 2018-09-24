package notifierNsq

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"time"

	"github.com/bitly/go-hostpool"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/mdata/notifierNsq/instrumented_nsq"
	"github.com/grafana/metrictank/stats"
	"github.com/nsqio/go-nsq"
	log "github.com/sirupsen/logrus"
)

var (
	hostPool  hostpool.HostPool
	producers map[string]*nsq.Producer
)

type NotifierNSQ struct {
	instance string
	in       chan mdata.SavedChunk
	buf      []mdata.SavedChunk
	metrics  mdata.Metrics
	idx      idx.MetricIndex
}

func New(instance string, metrics mdata.Metrics, idx idx.MetricIndex) *NotifierNSQ {
	// metric cluster.notifier.nsq.messages-published is a counter of messages published to the nsq cluster notifier
	messagesPublished = stats.NewCounter32("cluster.notifier.nsq.messages-published")
	// metric cluster.notifier.nsq.message_size is the sizes seen of messages through the nsq cluster notifier
	messagesSize = stats.NewMeter32("cluster.notifier.nsq.message_size", false)
	// producers
	hostPool = hostpool.NewEpsilonGreedy(nsqdAdds, 0, &hostpool.LinearEpsilonValueCalculator{})
	producers = make(map[string]*nsq.Producer)

	for _, addr := range nsqdAdds {
		producer, err := nsq.NewProducer(addr, pCfg)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err.Error(),
			}).Fatal("nsq-cluster: failed creating producer")
		}
		producers[addr] = producer
	}

	// consumers
	consumer, err := insq.NewConsumer(topic, channel, cCfg, "cluster.notifier.nsq.metric_persist.%s")
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Fatal("nsq-cluster: failed to create nsq consumer")
	}
	c := &NotifierNSQ{
		instance: instance,
		in:       make(chan mdata.SavedChunk),
		metrics:  metrics,
		idx:      idx,
	}
	consumer.AddConcurrentHandlers(c, 2)

	err = consumer.ConnectToNSQDs(nsqdAdds)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Fatal("nsq-cluster: failed to connect to nsqd")
	}
	log.Info("nsq-cluster: persist consumer connected to nsqd")

	err = consumer.ConnectToNSQLookupds(lookupdAdds)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Fatal("nsq-cluster: failed to connec to NSQLookupds")
	}
	go c.run()
	return c
}

func (c *NotifierNSQ) HandleMessage(m *nsq.Message) error {
	mdata.Handle(c.metrics, m.Body, c.idx)
	return nil
}

func (c *NotifierNSQ) Send(sc mdata.SavedChunk) {
	c.in <- sc
}

func (c *NotifierNSQ) run() {
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
func (c *NotifierNSQ) flush() {
	if len(c.buf) == 0 {
		return
	}

	msg := mdata.PersistMessageBatch{Instance: c.instance, SavedChunks: c.buf}
	c.buf = nil

	go func() {
		log.WithFields(log.Fields{
			"num.messages": len(msg.SavedChunks),
		}).Debug("CLU nsq-cluster: sending batch metricPersist messages")

		data, err := json.Marshal(&msg)
		if err != nil {
			log.Fatal("CLU nsq-cluster: failed to marshal persistMessage to json.")
		}
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, uint8(mdata.PersistMessageBatchV1))
		buf.Write(data)
		messagesSize.Value(buf.Len())

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
				log.WithFields(log.Fields{
					"host":  hostPoolResponse.Host(),
					"error": err.Error(),
				}).Warn("CLU nsq-cluster: publisher marking host as faulty")
			} else {
				sent = true
			}
			time.Sleep(time.Second)
		}
		messagesPublished.Inc()
	}()
}
