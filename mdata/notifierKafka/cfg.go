package notifierKafka

import (
	"flag"
	"time"

	"github.com/grafana/metrictank/kafka"
	"github.com/grafana/metrictank/stats"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"
)

var consumerConf *kafka.ConsumerConf
var Enabled bool
var backlogProcessTimeoutStr string
var backlogProcessTimeout time.Duration
var partitionScheme string
var topic string

// metric cluster.notifier.kafka.messages-published is a counter of messages published to the kafka cluster notifier
var messagesPublished = stats.NewCounter32("cluster.notifier.kafka.messages-published")

// metric cluster.notifier.kafka.message_size is the sizes seen of messages through the kafka cluster notifier
var messagesSize = stats.NewMeter32("cluster.notifier.kafka.message_size", false)

func init() {
	consumerConf = kafka.NewConfig()
	fs := flag.NewFlagSet("kafka-cluster", flag.ExitOnError)
	fs.BoolVar(&Enabled, "enabled", false, "")
	fs.DurationVar(&consumerConf.OffsetCommitInterval, "offset-commit-interval", time.Second*5, "Interval at which offsets should be saved.")
	fs.IntVar(&consumerConf.BatchNumMessages, "batch-num-messages", 10000, "Maximum number of messages batched in one MessageSet")
	fs.IntVar(&consumerConf.BufferMaxMs, "metrics-buffer-max-ms", 100, "Delay in milliseconds to wait for messages in the producer queue to accumulate before constructing message batches (MessageSets) to transmit to brokers")
	fs.IntVar(&consumerConf.ChannelBufferSize, "channel-buffer-size", 1000000, "Maximum number of messages allowed on the producer queue")
	fs.IntVar(&consumerConf.FetchMin, "consumer-fetch-min", 1, "Minimum number of bytes the broker responds with. If fetch.wait.max.ms expires the accumulated data will be sent to the client regardless of this setting")
	fs.IntVar(&consumerConf.MaxWaitMs, "consumer-max-wait-ms", 100, "Maximum time the broker may wait to fill the response with fetch.min.bytes")
	fs.IntVar(&consumerConf.MetadataBackoffTime, "metadata-backoff-time", 500, "Time to wait between attempts to fetch metadata in ms")
	fs.IntVar(&consumerConf.MetadataRetries, "metadata-retries", 5, "Number of retries to fetch metadata in case of failure")
	fs.IntVar(&consumerConf.MetadataTimeout, "consumer-metadata-timeout-ms", 10000, "Maximum time to wait for the broker to send its metadata in ms")
	fs.IntVar(&consumerConf.NetMaxOpenRequests, "net-max-open-requests", 100, "Maximum number of in-flight requests per broker connection. This is a generic property applied to all broker communication, however it is primarily relevant to produce requests.")
	fs.IntVar(&consumerConf.SessionTimeout, "consumer-session-timeout", 30000, "Client group session and failure detection timeout in ms")
	fs.StringVar(&backlogProcessTimeoutStr, "backlog-process-timeout", "60s", "Maximum time backlog processing can block during metrictank startup.")
	fs.StringVar(&consumerConf.Broker, "brokers", "kafka:9092", "tcp address for kafka (may be given multiple times as comma separated list)")
	fs.StringVar(&consumerConf.StartAtOffset, "offset", "oldest", "Set the offset to start consuming from. Can be one of newest, oldest or a time duration")
	fs.StringVar(&partitionScheme, "partition-scheme", "bySeries", "method used for partitioning metrics. This should match the settings of tsdb-gw. (byOrg|bySeries)")
	fs.StringVar(&consumerConf.Partitions, "partitions", "*", "kafka partitions to consume. use '*' or a comma separated list of id's. This should match the partitions used for kafka-mdm-in")
	fs.StringVar(&topic, "topic", "metricpersist", "kafka topic")
	globalconf.Register("kafka-cluster", fs)
}

func ConfigProcess() {
	if !Enabled {
		return
	}

	if consumerConf.OffsetCommitInterval == 0 {
		log.Fatal(4, "kafkamdm: offset-commit-interval must be greater then 0")
	}

	if consumerConf.MaxWaitMs == 0 {
		log.Fatal(4, "kafkamdm: consumer-max-wait-time must be greater then 0")
	}

	var err error
	backlogProcessTimeout, err = time.ParseDuration(backlogProcessTimeoutStr)
	if err != nil {
		log.Fatal(4, "kafka-cluster: unable to parse backlog-process-timeout. %s", err)
	}
}
