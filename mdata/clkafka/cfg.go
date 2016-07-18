package clkafka

import (
	"flag"
	"log"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/rakyll/globalconf"
)

var Enabled bool
var broker string
var topic string
var Brokers []string
var Topic string
var Topics []string
var Group string
var CConfig *cluster.Config
var PConfig *sarama.Config

func ConfigSetup() {
	inKafkaMdam := flag.NewFlagSet("kafka-cluster", flag.ExitOnError)
	inKafkaMdam.BoolVar(&Enabled, "enabled", false, "")
	inKafkaMdam.StringVar(&broker, "broker", "kafka:9092", "tcp address for kafka")
	inKafkaMdam.StringVar(&Topic, "topic", "metricpersist", "kafka topic")
	inKafkaMdam.StringVar(&Group, "group", "group1", "kafka consumer group")
	globalconf.Register("kafka-cluster", inKafkaMdam)
}

func ConfigProcess(instance string) {
	if !Enabled {
		return
	}
	Brokers = []string{broker}
	Topics = []string{Topic}

	CConfig = cluster.NewConfig()
	// see https://github.com/raintank/metrictank/issues/236
	CConfig.Consumer.Offsets.Initial = sarama.OffsetNewest
	CConfig.ClientID = instance + "-cluster"
	CConfig.Group.Return.Notifications = true
	CConfig.Config.Version = sarama.V0_10_0_0
	err := CConfig.Validate()
	if err != nil {
		log.Fatal(2, "kafka-cluster invalid consumer config: %s", err)
	}

	PConfig = sarama.NewConfig()
	PConfig.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	PConfig.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	PConfig.Producer.Compression = sarama.CompressionNone
	err = PConfig.Validate()
	if err != nil {
		log.Fatal(2, "kafka-cluster invalid producer config: %s", err)
	}
}
