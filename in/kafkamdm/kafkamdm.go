package kafkamdm

import (
	"flag"
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
var broker string
var topic string
var brokers []string
var topics []string
var group string
var config *cluster.Config

func ConfigSetup() {
	inKafkaMdm := flag.NewFlagSet("kafka-mdm-in", flag.ExitOnError)
	inKafkaMdm.BoolVar(&Enabled, "enabled", false, "")
	inKafkaMdm.StringVar(&broker, "broker", "kafka:9092", "tcp address for kafka")
	inKafkaMdm.StringVar(&topic, "topic", "mdm", "kafka topic")
	inKafkaMdm.StringVar(&group, "group", "group1", "kafka consumer group")
	globalconf.Register("kafka-mdm-in", inKafkaMdm)
}

func ConfigProcess(instance string) {
	if !Enabled {
		return
	}
	brokers = []string{broker}
	topics = []string{topic}

	config = cluster.NewConfig()
	// see https://github.com/raintank/metrictank/issues/236
	config.Consumer.Offsets.Initial = sarama.OffsetNewest
	config.ClientID = instance + "-mdm"
	config.Group.Return.Notifications = true
	config.ChannelBufferSize = 10000
	config.Consumer.Fetch.Min = 1024000     //1Mb
	config.Consumer.Fetch.Default = 4096000 //4Mb
	config.Consumer.MaxWaitTime = time.Second
	config.Net.MaxOpenRequests = 100
	config.Config.Version = sarama.V0_10_0_0
	err := config.Validate()
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

func (k *KafkaMdm) Start(metrics mdata.Metrics, defCache *defcache.DefCache, usg *usage.Usage) {
	k.In = in.New(metrics, defCache, usg, "kafka-mdm", k.stats)
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
