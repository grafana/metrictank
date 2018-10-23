package notifierKafka

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/grafana/metrictank/kafka"
	"github.com/grafana/metrictank/stats"
	"github.com/rakyll/globalconf"
	log "github.com/sirupsen/logrus"
)

var Enabled bool
var kafkaVersionStr string
var brokerStr string
var brokers []string
var topic string
var offsetStr string
var dataDir string
var config *sarama.Config
var offsetDuration time.Duration
var offsetCommitInterval time.Duration
var partitionStr string
var partitions []int32
var bootTimeOffsets map[int32]int64
var backlogProcessTimeout time.Duration
var backlogProcessTimeoutStr string
var partitionOffset map[int32]*stats.Gauge64
var partitionLogSize map[int32]*stats.Gauge64
var partitionLag map[int32]*stats.Gauge64

// metric cluster.notifier.kafka.messages-published is a counter of messages published to the kafka cluster notifier
var messagesPublished = stats.NewCounter32("cluster.notifier.kafka.messages-published")

// metric cluster.notifier.kafka.message_size is the sizes seen of messages through the kafka cluster notifier
var messagesSize = stats.NewMeter32("cluster.notifier.kafka.message_size", false)

func init() {
	fs := flag.NewFlagSet("kafka-cluster", flag.ExitOnError)
	fs.BoolVar(&Enabled, "enabled", false, "")
	fs.StringVar(&brokerStr, "brokers", "kafka:9092", "tcp address for kafka (may be given multiple times as comma separated list)")
	fs.StringVar(&kafkaVersionStr, "kafka-version", "0.10.0.0", "Kafka version in semver format. All brokers must be this version or newer.")
	fs.StringVar(&topic, "topic", "metricpersist", "kafka topic")
	fs.StringVar(&partitionStr, "partitions", "*", "kafka partitions to consume. use '*' or a comma separated list of id's. This should match the partitions used for kafka-mdm-in")
	fs.StringVar(&offsetStr, "offset", "last", "Set the offset to start consuming from. Can be one of newest, oldest,last or a time duration")
	fs.StringVar(&dataDir, "data-dir", "", "Directory to store partition offsets index")
	fs.DurationVar(&offsetCommitInterval, "offset-commit-interval", time.Second*5, "Interval at which offsets should be saved.")
	fs.StringVar(&backlogProcessTimeoutStr, "backlog-process-timeout", "60s", "Maximum time backlog processing can block during metrictank startup.")
	globalconf.Register("kafka-cluster", fs)
}

func ConfigProcess(instance string) {
	if !Enabled {
		return
	}

	kafkaVersion, err := sarama.ParseKafkaVersion(kafkaVersionStr)
	if err != nil {
		log.Fatalf("kafka-cluster: invalid kafka-version. %s", err)
	}

	switch offsetStr {
	case "last":
	case "oldest":
	case "newest":
	default:
		offsetDuration, err = time.ParseDuration(offsetStr)
		if err != nil {
			log.Fatalf("kafka-cluster: invalid offest format. %s", err)
		}
	}
	brokers = strings.Split(brokerStr, ",")

	config = sarama.NewConfig()
	config.ClientID = instance + "-cluster"
	config.Version = kafkaVersion
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewManualPartitioner
	err = config.Validate()
	if err != nil {
		log.Fatalf("kafka-cluster: invalid consumer config: %s", err)
	}

	backlogProcessTimeout, err = time.ParseDuration(backlogProcessTimeoutStr)
	if err != nil {
		log.Fatalf("kafka-cluster: unable to parse backlog-process-timeout. %s", err)
	}

	if partitionStr != "*" {
		parts := strings.Split(partitionStr, ",")
		for _, part := range parts {
			i, err := strconv.Atoi(part)
			if err != nil {
				log.Fatalf("kafka-cluster: could not parse partition %q. partitions must be '*' or a comma separated list of id's", part)
			}
			partitions = append(partitions, int32(i))
		}
	}
	// validate our partitions
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		log.Fatalf("kafka-cluster: failed to create client. %s", err)
	}
	defer client.Close()

	availParts, err := kafka.GetPartitions(client, []string{topic})
	if err != nil {
		log.Fatalf("kafka-cluster: %s", err.Error())
	}
	if partitionStr == "*" {
		partitions = availParts
	} else {
		missing := kafka.DiffPartitions(partitions, availParts)
		if len(missing) > 0 {
			log.Fatalf("kafka-cluster: configured partitions not in list of available partitions. missing %v", missing)
		}
	}

	// initialize our offset metrics
	partitionOffset = make(map[int32]*stats.Gauge64)
	partitionLogSize = make(map[int32]*stats.Gauge64)
	partitionLag = make(map[int32]*stats.Gauge64)

	// get the "newest" offset for all partitions.
	// when booting up, we will delay consuming metrics until we have
	// caught up to these offsets.
	bootTimeOffsets = make(map[int32]int64)
	for _, part := range partitions {
		offset, err := client.GetOffset(topic, part, sarama.OffsetNewest)
		if err != nil {
			log.Fatalf("kafka-cluster: failed to get newest offset for topic %s part %d: %s", topic, part, err)
		}
		bootTimeOffsets[part] = offset
		// metric cluster.notifier.kafka.partition.%d.offset is the current offset for the partition (%d) that we have consumed
		partitionOffset[part] = stats.NewGauge64(fmt.Sprintf("cluster.notifier.kafka.partition.%d.offset", part))
		// metric cluster.notifier.kafka.partition.%d.log_size is the size of the kafka partition (%d), aka the newest available offset.
		partitionLogSize[part] = stats.NewGauge64(fmt.Sprintf("cluster.notifier.kafka.partition.%d.log_size", part))
		// metric cluster.notifier.kafka.partition.%d.lag is how many messages (mechunkWriteRequestsrics) there are in the kafka
		// partition (%d) that we have not yet consumed.
		partitionLag[part] = stats.NewGauge64(fmt.Sprintf("cluster.notifier.kafka.partition.%d.lag", part))
	}
	log.Infof("kafka-cluster: consuming from partitions %v", partitions)
}
