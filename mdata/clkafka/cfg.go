package clkafka

import (
	"flag"
	"log"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/rakyll/globalconf"
)

var Enabled bool
var brokerStr string
var Brokers []string
var Topic string
var Topics []string
var Group string
var CConfig *cluster.Config
var PConfig *sarama.Config

func ConfigSetup() {
	fs := flag.NewFlagSet("kafka-cluster", flag.ExitOnError)
	fs.BoolVar(&Enabled, "enabled", false, "")
	fs.StringVar(&brokerStr, "brokers", "kafka:9092", "tcp address for kafka (may be given multiple times as comma separated list)")
	fs.StringVar(&Topic, "topic", "metricpersist", "kafka topic")
	fs.StringVar(&Group, "group", "group1", "kafka consumer group")
	globalconf.Register("kafka-cluster", fs)
}

func ConfigProcess(instance string) {
	if !Enabled {
		return
	}
	Brokers = strings.Split(brokerStr, ",")
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
	PConfig.ClientID = instance + "-cluster"
	err = PConfig.Validate()
	if err != nil {
		log.Fatal(2, "kafka-cluster invalid producer config: %s", err)
	}
}
