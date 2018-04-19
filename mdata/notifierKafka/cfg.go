package notifierKafka

import (
	"flag"
	"time"

	"github.com/grafana/metrictank/kafka"
	"github.com/grafana/metrictank/stats"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"
)

var clientConf *kafka.ClientConf
var Enabled bool
var backlogProcessTimeout time.Duration
var partitionScheme string
var topic string

// metric cluster.notifier.kafka.messages-published is a counter of messages published to the kafka cluster notifier
var messagesPublished = stats.NewCounter32("cluster.notifier.kafka.messages-published")

// metric cluster.notifier.kafka.message_size is the sizes seen of messages through the kafka cluster notifier
var messagesSize = stats.NewMeter32("cluster.notifier.kafka.message_size", false)

func init() {
	clientConf = kafka.NewConfig()
	fs := flag.NewFlagSet("kafka-cluster", flag.ExitOnError)
	fs.BoolVar(&Enabled, "enabled", false, "")
	fs.DurationVar(&clientConf.LagCollectionInterval, "lag-collection-interval", time.Second*5, "Interval at which the lag is calculated and saved")
	fs.IntVar(&clientConf.BatchNumMessages, "batch-num-messages", 10000, "Maximum number of messages batched in one MessageSet")
	fs.DurationVar(&clientConf.BufferMax, "metrics-buffer-max", time.Millisecond*100, "Delay to wait for messages in the producer queue to accumulate before constructing message batches (MessageSets) to transmit to brokers")
	fs.IntVar(&clientConf.ChannelBufferSize, "channel-buffer-size", 1000000, "Maximum number of messages allowed on the producer queue")
	fs.IntVar(&clientConf.FetchMin, "fetch-min", 1, "Minimum number of bytes the broker responds with. If fetch.wait.max.ms expires the accumulated data will be sent to the client regardless of this setting")
	fs.DurationVar(&clientConf.MaxWait, "max-wait", time.Millisecond*100, "Maximum time the broker may wait to fill the response with fetch.min.bytes")
	fs.DurationVar(&clientConf.MetadataBackoffTime, "metadata-backoff-time", time.Millisecond*500, "Time to wait between attempts to fetch metadata")
	fs.IntVar(&clientConf.MetadataRetries, "metadata-retries", 5, "Number of retries to fetch metadata in case of failure")
	fs.DurationVar(&clientConf.MetadataTimeout, "metadata-timeout", time.Second*10, "Maximum time to wait for the broker to send its metadata")
	fs.IntVar(&clientConf.NetMaxOpenRequests, "net-max-open-requests", 100, "Maximum number of in-flight requests per broker connection. This is a generic property applied to all broker communication, however it is primarily relevant to produce requests.")
	fs.DurationVar(&clientConf.SessionTimeout, "session-timeout", time.Second*30, "Client group session and failure detection timeout")
	fs.DurationVar(&backlogProcessTimeout, "backlog-process-timeout", time.Second*60, "Maximum time backlog processing can block during metrictank startup.")
	fs.StringVar(&clientConf.Broker, "brokers", "kafka:9092", "tcp address for kafka (may be given multiple times as comma separated list)")
	fs.StringVar(&clientConf.StartAtOffset, "offset", "oldest", "Set the offset to start consuming from. Can be one of newest, oldest or a time duration")
	fs.StringVar(&partitionScheme, "partition-scheme", "bySeries", "method used for partitioning metrics. This should match the settings of tsdb-gw. (byOrg|bySeries)")
	fs.StringVar(&clientConf.Partitions, "partitions", "*", "kafka partitions to consume. use '*' or a comma separated list of id's. This should match the partitions used for kafka-mdm-in")
	fs.StringVar(&topic, "topic", "metricpersist", "kafka topic")
	globalconf.Register("kafka-cluster", fs)
}

func ConfigProcess() {
	if !Enabled {
		return
	}

	if clientConf.LagCollectionInterval == 0 {
		log.Fatal(4, "kafkamdm: lag-collection-interval must be greater then 0")
	}

	if clientConf.MaxWait == 0 {
		log.Fatal(4, "kafkamdm: max-wait-time must be greater then 0")
	}

	if !clientConf.OffsetIsValid() {
		log.Fatal(4, "kafkamdm: offset %s is not valid", clientConf.StartAtOffset)
	}
}
