package notifierKafka

import (
	"flag"
	"log"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/rakyll/globalconf"
)

var Enabled bool
var brokerStr string
var Brokers []string
var Topic string
var OffsetStr string
var DataDir string
var Config *sarama.Config
var OffsetDuration time.Duration
var OffsetCommitInterval time.Duration

func ConfigSetup() {
	fs := flag.NewFlagSet("kafka-cluster", flag.ExitOnError)
	fs.BoolVar(&Enabled, "enabled", false, "")
	fs.StringVar(&brokerStr, "brokers", "kafka:9092", "tcp address for kafka (may be given multiple times as comma separated list)")
	fs.StringVar(&Topic, "topic", "metricpersist", "kafka topic")
	fs.StringVar(&OffsetStr, "offset", "last", "Set the offset to start consuming from. Can be one of newest, oldest,last or a time duration")
	fs.StringVar(&DataDir, "data-dir", "", "Directory to store partition offsets index")
	fs.DurationVar(&OffsetCommitInterval, "offset-commit-interval", time.Second*5, "Interval at which offsets should be saved.")
	globalconf.Register("kafka-cluster", fs)
}

func ConfigProcess(instance string) {
	if !Enabled {
		return
	}
	var err error
	switch OffsetStr {
	case "last":
	case "oldest":
	case "newest":
	default:
		OffsetDuration, err = time.ParseDuration(OffsetStr)
		if err != nil {
			log.Fatal(4, "kafka-cluster: invalid offest format. %s", err)
		}
	}
	Brokers = strings.Split(brokerStr, ",")

	Config = sarama.NewConfig()
	Config.ClientID = instance + "-cluster"
	Config.Version = sarama.V0_10_0_0
	Config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	Config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	Config.Producer.Compression = sarama.CompressionNone
	err = Config.Validate()
	if err != nil {
		log.Fatal(2, "kafka-cluster invalid consumer config: %s", err)
	}
}
