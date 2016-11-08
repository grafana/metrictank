package kafkamdam

import (
	"flag"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"

	"github.com/bsm/sarama-cluster"
	"github.com/raintank/met"
	"github.com/raintank/metrictank/idx"
	"github.com/raintank/metrictank/in"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/usage"
	"gopkg.in/raintank/schema.v1"
	schemaMsg "gopkg.in/raintank/schema.v1/msg"
)

type KafkaMdam struct {
	in.In
	consumer *cluster.Consumer
	stats    met.Backend

	wg sync.WaitGroup
	// read from this channel to block until consumer is cleanly stopped
	StopChan chan int
}

func (k *KafkaMdam) Name() string {
	return "kafkaMdam"
}

var Enabled bool
var brokerStr string
var brokers []string
var topicStr string
var topics []string
var group string
var config *cluster.Config

func ConfigSetup() {
	inKafkaMdam := flag.NewFlagSet("kafka-mdam-in", flag.ExitOnError)
	inKafkaMdam.BoolVar(&Enabled, "enabled", false, "")
	inKafkaMdam.StringVar(&brokerStr, "brokers", "kafka:9092", "tcp address for kafka (may be be given multiple times as a comma-separated list)")
	inKafkaMdam.StringVar(&topicStr, "topics", "mdam", "kafka topic (may be given multiple times as a comma-separated list)")
	inKafkaMdam.StringVar(&group, "group", "group1", "kafka consumer group")
	globalconf.Register("kafka-mdam-in", inKafkaMdam)
}

func ConfigProcess(instance string) {
	if !Enabled {
		return
	}
	brokers = strings.Split(brokerStr, ",")
	topics = strings.Split(topicStr, ",")

	config := cluster.NewConfig()
	// see https://github.com/raintank/metrictank/issues/236
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.ClientID = instance + "-mdam"
	config.Group.Return.Notifications = true
	config.Config.Version = sarama.V0_10_0_0
	err := config.Validate()
	if err != nil {
		log.Fatal(2, "kafka-mdam invalid config: %s", err)
	}
}

func New(stats met.Backend) *KafkaMdam {
	consumer, err := cluster.NewConsumer(brokers, group, topics, config)
	if err != nil {
		log.Fatal(2, "kafka-mdam failed to start consumer: %s", err)
	}
	log.Info("kafka-mdam consumer started without error")
	k := KafkaMdam{
		consumer: consumer,
		stats:    stats,
		StopChan: make(chan int),
	}

	return &k
}

func (k *KafkaMdam) Start(metrics mdata.Metrics, metricIndex idx.MetricIndex, usg *usage.Usage) {
	k.In = in.New(metrics, metricIndex, usg, "kafka-mdam", k.stats)
	go k.notifications()
	go k.consume()
}

func (k *KafkaMdam) consume() {
	k.wg.Add(1)
	messageChan := k.consumer.Messages()
	tmp := schemaMsg.MetricData{Metrics: make([]*schema.MetricData, 1)}
	for msg := range messageChan {
		log.Debug("kafka-mdam received message: Topic %s, Partition: %d, Offset: %d, Key: %x", msg.Topic, msg.Partition, msg.Offset, msg.Key)
		k.handleMsg(msg.Value, &tmp)
		k.consumer.MarkOffset(msg, "")
	}
	log.Info("kafka-mdam consumer ended.")
	k.wg.Done()
}

func (k *KafkaMdam) handleMsg(data []byte, tmp *schemaMsg.MetricData) {
	err := tmp.InitFromMsg(data)
	if err != nil {
		k.In.MetricsDecodeErr.Inc(1)
		log.Error(3, "skipping message. %s", err)
		return
	}
	k.In.MsgsAge.Value(time.Now().Sub(tmp.Produced).Nanoseconds() / 1000)
	err = tmp.DecodeMetricData() // reads metrics from in.tmp.Msg and unsets it
	if err != nil {
		k.In.MetricsDecodeErr.Inc(1)
		log.Error(3, "skipping message. %s", err)
		return
	}
	k.In.MetricsPerMessage.Value(int64(len(tmp.Metrics)))
	for _, metric := range tmp.Metrics {
		k.In.Process(metric)
	}
}

func (k *KafkaMdam) notifications() {
	k.wg.Add(1)
	for msg := range k.consumer.Notifications() {
		if len(msg.Claimed) > 0 {
			for topic, partitions := range msg.Claimed {
				log.Info("kafka-mdam consumer claimed %d partitions on topic: %s", len(partitions), topic)
			}
		}
		if len(msg.Released) > 0 {
			for topic, partitions := range msg.Released {
				log.Info("kafka-mdam consumer released %d partitions on topic: %s", len(partitions), topic)
			}
		}

		if len(msg.Current) == 0 {
			log.Info("kafka-mdam consumer is no longer consuming from any partitions.")
		} else {
			log.Info("kafka-mdam Current partitions:")
			for topic, partitions := range msg.Current {
				log.Info("kafka-mdam Current partitions: %s: %v", topic, partitions)
			}
		}
	}
	log.Info("kafka-mdam notification processing stopped")
	k.wg.Done()
}

// Stop will initiate a graceful stop of the Consumer (permanent)
// and block until it is stopped.
func (k *KafkaMdam) Stop() {
	// closes notifications and messages channels, amongst others
	k.consumer.Close()
	k.wg.Wait()
}
