package kafkaall

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"flag"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"

	"github.com/bsm/sarama-cluster"
	"github.com/raintank/met"
	"github.com/raintank/metrictank/defcache"
	"github.com/raintank/metrictank/in"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/usage"
)

var (
	messagesPublished met.Count
	messagesSize      met.Meter
	sendErrProducer   met.Count
	sendErrOther      met.Count
)

// KafkaAll is a consumer for metrics, as well as producer and consumer for cluster messages
// all using the same topic, this enforces strong ordering between cluster msgs and metric msgs.
type KafkaAll struct {
	in.In
	in       chan mdata.SavedChunk
	consumer *cluster.Consumer
	producer sarama.SyncProducer
	stats    met.Backend
	buf      []mdata.SavedChunk

	instance string
	mdata.Cl

	wg sync.WaitGroup
	// read from this channel to block until consumer is cleanly stopped
	StopChan chan int
}

var LogLevel int
var Enabled bool
var brokerStr string
var brokers []string
var topicStr string
var topics []string
var group string
var cconfig *cluster.Config
var pconfig *sarama.Config
var channelBufferSize int
var consumerFetchMin int
var consumerFetchDefault int
var consumerMaxWaitTime string
var consumerMaxProcessingTime string
var netMaxOpenRequests int

func ConfigSetup() {
	fs := flag.NewFlagSet("kafka-all", flag.ExitOnError)
	fs.BoolVar(&Enabled, "enabled", false, "")
	fs.StringVar(&brokerStr, "brokers", "kafka:9092", "tcp address for kafka (may be be given multiple times as a comma-separated list)")
	fs.StringVar(&topicStr, "topics", "mdm", "kafka topic (may be given multiple times as a comma-separated list. first one will be used for cluster production)")
	fs.StringVar(&group, "group", "group1", "kafka consumer group")
	fs.IntVar(&channelBufferSize, "channel-buffer-size", 1000000, "The number of metrics to buffer in internal and external channels")
	fs.IntVar(&consumerFetchMin, "consumer-fetch-min", 1024000, "The minimum number of message bytes to fetch in a request")
	fs.IntVar(&consumerFetchDefault, "consumer-fetch-default", 4096000, "The default number of message bytes to fetch in a request")
	fs.StringVar(&consumerMaxWaitTime, "consumer-max-wait-time", "1s", "The maximum amount of time the broker will wait for Consumer.Fetch.Min bytes to become available before it returns fewer than that anyway")
	fs.StringVar(&consumerMaxProcessingTime, "consumer-max-processing-time", "1s", "The maximum amount of time the consumer expects a message takes to process")
	fs.IntVar(&netMaxOpenRequests, "net-max-open-requests", 100, "How many outstanding requests a connection is allowed to have before sending on it blocks")
	globalconf.Register("kafka-all", fs)
}

func ConfigProcess(instance string) {
	if !Enabled {
		return
	}

	waitTime, err := time.ParseDuration(consumerMaxWaitTime)
	if err != nil {
		log.Fatal(4, "kafka-all invalid config, could not parse consumer-max-wait-time: %s", err)
	}
	processingTime, err := time.ParseDuration(consumerMaxProcessingTime)
	if err != nil {
		log.Fatal(4, "kafka-all invalid config, could not parse consumer-max-processing-time: %s", err)
	}

	brokers = strings.Split(brokerStr, ",")
	topics = strings.Split(topicStr, ",")

	cconfig = cluster.NewConfig()
	// see https://github.com/raintank/metrictank/issues/236
	cconfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	cconfig.ClientID = instance + "-all"
	cconfig.Group.Return.Notifications = true
	cconfig.ChannelBufferSize = channelBufferSize
	cconfig.Consumer.Fetch.Min = int32(consumerFetchMin)
	cconfig.Consumer.Fetch.Default = int32(consumerFetchDefault)
	cconfig.Consumer.MaxWaitTime = waitTime
	cconfig.Consumer.MaxProcessingTime = processingTime
	cconfig.Net.MaxOpenRequests = netMaxOpenRequests
	cconfig.Config.Version = sarama.V0_10_0_0
	err = cconfig.Validate()
	if err != nil {
		log.Fatal(2, "kafka-all invalid config: %s", err)
	}

	pconfig = sarama.NewConfig()
	pconfig.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	pconfig.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	pconfig.Producer.Compression = sarama.CompressionNone
	pconfig.ClientID = instance + "-cluster"
	err = pconfig.Validate()
	if err != nil {
		log.Fatal(2, "kafka-cluster invalid producer config: %s", err)
	}
}

func New(instance string, metrics mdata.Metrics, stats met.Backend) *KafkaAll {
	consumer, err := cluster.NewConsumer(brokers, group, topics, cconfig)
	if err != nil {
		log.Fatal(2, "kafka-all failed to start consumer: %s", err)
	}
	log.Info("kafka-all consumer started without error")

	producer, err := sarama.NewSyncProducer(brokers, pconfig)
	if err != nil {
		log.Fatal(2, "kafka-all failed to start producer: %s", err)
	}

	k := KafkaAll{
		in:       make(chan mdata.SavedChunk),
		producer: producer,
		consumer: consumer,
		stats:    stats,
		instance: instance,
		Cl:       mdata.NewCl(instance, metrics),
		StopChan: make(chan int),
	}

	messagesPublished = stats.NewCount("cluster.messages-published")
	messagesSize = stats.NewMeter("cluster.message_size", 0)
	sendErrProducer = stats.NewCount("cluster.errors.producer")
	sendErrOther = stats.NewCount("cluster.errors.other")

	return &k
}

func (k *KafkaAll) Start(metrics mdata.Metrics, defCache *defcache.DefCache, usg *usage.Usage) {
	k.In = in.New(metrics, defCache, usg, "kafka-all", k.stats)
	go k.notifications()
	go k.consume()
}

func (k *KafkaAll) consume() {
	k.wg.Add(1)
	messageChan := k.consumer.Messages()
	for msg := range messageChan {
		if LogLevel < 2 {
			log.Debug("kafka-all received message: Topic %s, Partition: %d, Offset: %d, Key: %x", msg.Topic, msg.Partition, msg.Offset, msg.Key)
		}
		// TODO: figure out which it is
		// persist:
		k.Cl.Handle(msg.Value)
		// regular:
		k.In.Handle(msg.Key, msg.Value)

		k.consumer.MarkOffset(msg, "")
	}
	log.Info("kafka-all consumer ended.")
	k.wg.Done()
}

func (k *KafkaAll) notifications() {
	k.wg.Add(1)
	for msg := range k.consumer.Notifications() {
		if len(msg.Claimed) > 0 {
			for topic, partitions := range msg.Claimed {
				log.Info("kafka-all consumer claimed %d partitions on topic: %s", len(partitions), topic)
			}
		}
		if len(msg.Released) > 0 {
			for topic, partitions := range msg.Released {
				log.Info("kafka-all consumer released %d partitions on topic: %s", len(partitions), topic)
			}
		}

		if len(msg.Current) == 0 {
			log.Info("kafka-all consumer is no longer consuming from any partitions.")
		} else {
			log.Info("kafka-all Current partitions:")
			for topic, partitions := range msg.Current {
				log.Info("kafka-all Current partitions: %s: %v", topic, partitions)
			}
		}
	}
	log.Info("kafka-all notification processing stopped")
	k.wg.Done()
}

// Stop will initiate a graceful stop of the Consumer (permanent)
//
// NOTE: receive on StopChan to block until this process completes
func (k *KafkaAll) Stop() {
	// closes notifications and messages channels, amongst others
	k.consumer.Close()
	k.producer.Close()

	go func() {
		k.wg.Wait()
		close(k.StopChan)
	}()
}

func (c *KafkaAll) Send(sc mdata.SavedChunk) {
	c.in <- sc
}

func (c *KafkaAll) produce() {
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
			if len(c.buf) != 0 {
				c.flush()
			}
		}
	}
}

// flush makes sure the batch gets sent, asynchronously.
func (c *KafkaAll) flush() {

	log.Debug("CLU kafka-all sending %d batch metricPersist messages", len(c.buf))

	payload := make([]*sarama.ProducerMessage, len(c.buf))
	//pre := time.Now()

	for i, sc := range c.buf {
		msg := mdata.PersistMessage{Instance: c.instance, Key: sc.Key, T0: sc.T0}
		data, err := json.Marshal(&msg)
		if err != nil {
			log.Fatal(4, "CLU kafka-all failed to marshal persistMessage to json.")
		}

		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, uint8(mdata.PersistMessageBatchV1))
		buf.Write(data)
		messagesSize.Value(int64(buf.Len()))

		payload[i] = &sarama.ProducerMessage{
			Key:   sarama.ByteEncoder(sc.PartKey),
			Topic: topics[0],
			Value: sarama.ByteEncoder(buf.Bytes()),
		}
		messagesSize.Value(int64(len(data)))
	}
	c.buf = nil
	err := c.producer.SendMessages(payload)
	if err != nil {
		if errors, ok := err.(sarama.ProducerErrors); ok {
			sendErrProducer.Inc(int64(len(errors)))
			for i := 0; i < 10 && i < len(errors); i++ {
				log.Error(4, "SendMessages ProducerError %d/%d: %s", i, len(errors), errors[i].Error())
			}
		} else {
			sendErrOther.Inc(1)
			log.Error(4, "SendMessages error: %s", err.Error())
		}
		return
	}
	messagesPublished.Inc(int64(len(payload)))
}
