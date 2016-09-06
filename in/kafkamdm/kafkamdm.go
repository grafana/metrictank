package kafkamdm

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
)

type KafkaMdm struct {
	in.In
	consumer *cluster.Consumer
	stats    met.Backend

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
var config *cluster.Config
var channelBufferSize int
var consumerFetchMin int
var consumerFetchDefault int
var consumerMaxWaitTime string
var consumerMaxProcessingTime string
var netMaxOpenRequests int

func ConfigSetup() {
	inKafkaMdm := flag.NewFlagSet("kafka-mdm-in", flag.ExitOnError)
	inKafkaMdm.BoolVar(&Enabled, "enabled", false, "")
	inKafkaMdm.StringVar(&brokerStr, "brokers", "kafka:9092", "tcp address for kafka (may be be given multiple times as a comma-separated list)")
	inKafkaMdm.StringVar(&topicStr, "topics", "mdm", "kafka topic (may be given multiple times as a comma-separated list)")
	inKafkaMdm.StringVar(&group, "group", "group1", "kafka consumer group")
	inKafkaMdm.IntVar(&channelBufferSize, "channel-buffer-size", 1000000, "The number of metrics to buffer in internal and external channels")
	inKafkaMdm.IntVar(&consumerFetchMin, "consumer-fetch-min", 1, "The minimum number of message bytes to fetch in a request")
	inKafkaMdm.IntVar(&consumerFetchDefault, "consumer-fetch-default", 4096000, "The default number of message bytes to fetch in a request")
	inKafkaMdm.StringVar(&consumerMaxWaitTime, "consumer-max-wait-time", "1s", "The maximum amount of time the broker will wait for Consumer.Fetch.Min bytes to become available before it returns fewer than that anyway")
	inKafkaMdm.StringVar(&consumerMaxProcessingTime, "consumer-max-processing-time", "1s", "The maximum amount of time the consumer expects a message takes to process")
	inKafkaMdm.IntVar(&netMaxOpenRequests, "net-max-open-requests", 100, "How many outstanding requests a connection is allowed to have before sending on it blocks")
	globalconf.Register("kafka-mdm-in", inKafkaMdm)
}

func ConfigProcess(instance string) {
	if !Enabled {
		return
	}

	waitTime, err := time.ParseDuration(consumerMaxWaitTime)
	if err != nil {
		log.Fatal(4, "kafka-mdm invalid config, could not parse consumer-max-wait-time: %s", err)
	}
	processingTime, err := time.ParseDuration(consumerMaxProcessingTime)
	if err != nil {
		log.Fatal(4, "kafka-mdm invalid config, could not parse consumer-max-processing-time: %s", err)
	}

	brokers = strings.Split(brokerStr, ",")
	topics = strings.Split(topicStr, ",")

	config = cluster.NewConfig()
	// see https://github.com/raintank/metrictank/issues/236
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.ClientID = instance + "-mdm"
	config.Group.Return.Notifications = true
	config.ChannelBufferSize = channelBufferSize
	config.Consumer.Fetch.Min = int32(consumerFetchMin)
	config.Consumer.Fetch.Default = int32(consumerFetchDefault)
	config.Consumer.MaxWaitTime = waitTime
	config.Consumer.MaxProcessingTime = processingTime
	config.Net.MaxOpenRequests = netMaxOpenRequests
	config.Config.Version = sarama.V0_10_0_0
	err = config.Validate()
	if err != nil {
		log.Fatal(2, "kafka-mdm invalid config: %s", err)
	}
}

func New(stats met.Backend) *KafkaMdm {
	consumer, err := cluster.NewConsumer(brokers, group, topics, config)
	if err != nil {
		log.Fatal(2, "kafka-mdm failed to start consumer: %s", err)
	}
	log.Info("kafka-mdm consumer started without error")
	k := KafkaMdm{
		consumer: consumer,
		stats:    stats,
		StopChan: make(chan int),
	}

	return &k
}

func (k *KafkaMdm) Start(metrics mdata.Metrics, metricIndex idx.MetricIndex, usg *usage.Usage) {
	k.In = in.New(metrics, metricIndex, usg, "kafka-mdm", k.stats)
	go k.notifications()
	go k.consume()
}

func (k *KafkaMdm) consume() {
	k.wg.Add(1)
	messageChan := k.consumer.Messages()
	for msg := range messageChan {
		if LogLevel < 2 {
			log.Debug("kafka-mdm received message: Topic %s, Partition: %d, Offset: %d, Key: %x", msg.Topic, msg.Partition, msg.Offset, msg.Key)
		}
		k.In.Handle(msg.Value)
		k.consumer.MarkOffset(msg, "")
	}
	log.Info("kafka-mdm consumer ended.")
	k.wg.Done()
}

func (k *KafkaMdm) notifications() {
	k.wg.Add(1)
	for msg := range k.consumer.Notifications() {
		if len(msg.Claimed) > 0 {
			for topic, partitions := range msg.Claimed {
				log.Info("kafka-mdm consumer claimed %d partitions on topic: %s", len(partitions), topic)
			}
		}
		if len(msg.Released) > 0 {
			for topic, partitions := range msg.Released {
				log.Info("kafka-mdm consumer released %d partitions on topic: %s", len(partitions), topic)
			}
		}

		if len(msg.Current) == 0 {
			log.Info("kafka-mdm consumer is no longer consuming from any partitions.")
		} else {
			log.Info("kafka-mdm Current partitions:")
			for topic, partitions := range msg.Current {
				log.Info("kafka-mdm Current partitions: %s: %v", topic, partitions)
			}
		}
	}
	log.Info("kafka-mdm notification processing stopped")
	k.wg.Done()
}

// Stop will initiate a graceful stop of the Consumer (permanent)
//
// NOTE: receive on StopChan to block until this process completes
func (k *KafkaMdm) Stop() {
	// closes notifications and messages channels, amongst others
	k.consumer.Close()

	go func() {
		k.wg.Wait()
		close(k.StopChan)
	}()
}
