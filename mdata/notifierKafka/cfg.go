package notifierKafka

import (
	"flag"
	"log"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/raintank/met"
	"github.com/rakyll/globalconf"
)

var Enabled bool
var brokerStr string
var brokers []string
var topic string
var offsetStr string
var dataDir string
var config *sarama.Config
var offsetDuration time.Duration
var offsetCommitInterval time.Duration

var messagesPublished met.Count
var messagesSize met.Meter

func ConfigSetup() {
	fs := flag.NewFlagSet("kafka-cluster", flag.ExitOnError)
	fs.BoolVar(&Enabled, "enabled", false, "")
	fs.StringVar(&brokerStr, "brokers", "kafka:9092", "tcp address for kafka (may be given multiple times as comma separated list)")
	fs.StringVar(&topic, "topic", "metricpersist", "kafka topic")
	fs.StringVar(&offsetStr, "offset", "last", "Set the offset to start consuming from. Can be one of newest, oldest,last or a time duration")
	fs.StringVar(&dataDir, "data-dir", "", "Directory to store partition offsets index")
	fs.DurationVar(&offsetCommitInterval, "offset-commit-interval", time.Second*5, "Interval at which offsets should be saved.")
	globalconf.Register("kafka-cluster", fs)
}

func ConfigProcess(instance string) {
	if !Enabled {
		return
	}
	var err error
	switch offsetStr {
	case "last":
	case "oldest":
	case "newest":
	default:
		offsetDuration, err = time.ParseDuration(offsetStr)
		if err != nil {
			log.Fatal(4, "kafka-cluster: invalid offest format. %s", err)
		}
	}
	brokers = strings.Split(brokerStr, ",")

	config = sarama.NewConfig()
	config.ClientID = instance + "-cluster"
	config.Version = sarama.V0_10_0_0
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Compression = sarama.CompressionNone
	err = config.Validate()
	if err != nil {
		log.Fatal(2, "kafka-cluster invalid consumer config: %s", err)
	}
}
