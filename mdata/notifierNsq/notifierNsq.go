package notifierNsq

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"time"

	"github.com/bitly/go-hostpool"
	"github.com/nsqio/go-nsq"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/mdata/notifierNsq/instrumented_nsq"
	"github.com/raintank/metrictank/stats"
	"github.com/raintank/worldping-api/pkg/log"
)

var (
	hostPool  hostpool.HostPool
	producers map[string]*nsq.Producer
)

type NotifierNSQ struct {
	in       chan mdata.SavedChunk
	buf      []mdata.SavedChunk
	instance string
	mdata.Notifier
}

func New(instance string, metrics mdata.Metrics) *NotifierNSQ {
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
			log.Fatal(4, "nsq-cluster failed creating producer %s", err.Error())
		}
		producers[addr] = producer
	}

	// consumers
	consumer, err := insq.NewConsumer(topic, channel, cCfg, "cluster.notifier.nsq.metric_persist.%s")
	if err != nil {
		log.Fatal(4, "nsq-cluster failed to create NSQ consumer. %s", err)
	}
	c := &NotifierNSQ{
		in:       make(chan mdata.SavedChunk),
		instance: instance,
		Notifier: mdata.Notifier{
			Instance: instance,
			Metrics:  metrics,
		},
	}
	consumer.AddConcurrentHandlers(c, 2)

	err = consumer.ConnectToNSQDs(nsqdAdds)
	if err != nil {
		log.Fatal(4, "nsq-cluster failed to connect to NSQDs. %s", err)
	}
	log.Info("nsq-cluster persist consumer connected to nsqd")

	err = consumer.ConnectToNSQLookupds(lookupdAdds)
	if err != nil {
		log.Fatal(4, "nsq-cluster failed to connect to NSQLookupds. %s", err)
	}
	go c.run()
	return c
}

func (c *NotifierNSQ) HandleMessage(m *nsq.Message) error {
	c.Handle(m.Body)
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
		log.Debug("CLU nsq-cluster sending %d batch metricPersist messages", len(msg.SavedChunks))

		data, err := json.Marshal(&msg)
		if err != nil {
			log.Fatal(4, "CLU nsq-cluster failed to marshal persistMessage to json.")
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
				log.Warn("CLU nsq-cluster publisher marking host %s as faulty due to %s", hostPoolResponse.Host(), err)
			} else {
				sent = true
			}
			time.Sleep(time.Second)
		}
		messagesPublished.Inc()
	}()
}
