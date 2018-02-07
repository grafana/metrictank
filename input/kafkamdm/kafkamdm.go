package kafkamdm

import (
	"flag"
	"strings"
	"sync"
	"time"

	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/input"
	"github.com/grafana/metrictank/kafka"
	"github.com/grafana/metrictank/stats"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"
	schema "gopkg.in/raintank/schema.v1"
	"gopkg.in/raintank/schema.v1/msg"
)

// metric input.kafka-mdm.metrics_per_message is how many metrics per message were seen.
var metricsPerMessage = stats.NewMeter32("input.kafka-mdm.metrics_per_message", false)

// metric input.kafka-mdm.metrics_decode_err is a count of times an input message failed to parse
var metricsDecodeErr = stats.NewCounter32("input.kafka-mdm.metrics_decode_err")

type KafkaMdm struct {
	input.Handler
	consumer *kafka.Consumer
	wg       sync.WaitGroup

	// signal to PartitionConsumers to shutdown
	stopChan chan struct{}
	// signal to caller that it should shutdown
	fatal chan struct{}
}

func (k *KafkaMdm) Name() string {
	return "kafka-mdm"
}

var consumerConf *kafka.ConsumerConf
var Enabled bool
var orgId uint
var LogLevel int
var topicStr string

func ConfigSetup() {
	consumerConf = kafka.NewConfig()
	inKafkaMdm := flag.NewFlagSet("kafka-mdm-in", flag.ExitOnError)
	inKafkaMdm.BoolVar(&Enabled, "enabled", false, "")
	inKafkaMdm.UintVar(&orgId, "org-id", 0, "For incoming MetricPoint messages without org-id, assume this org id")
	inKafkaMdm.DurationVar(&consumerConf.OffsetCommitInterval, "offset-commit-interval", time.Second*5, "Interval at which offsets should be saved.")
	inKafkaMdm.IntVar(&consumerConf.BatchNumMessages, "batch-num-messages", 10000, "Maximum number of messages batched in one MessageSet")
	inKafkaMdm.IntVar(&consumerConf.BufferMaxMs, "metrics-buffer-max-ms", 100, "Delay in milliseconds to wait for messages in the producer queue to accumulate before constructing message batches (MessageSets) to transmit to brokers")
	inKafkaMdm.IntVar(&consumerConf.ChannelBufferSize, "channel-buffer-size", 1000, "Maximum number of messages allowed on the producer queue")
	inKafkaMdm.IntVar(&consumerConf.FetchMin, "consumer-fetch-min", 1, "Minimum number of bytes the broker responds with. If fetch.wait.max.ms expires the accumulated data will be sent to the client regardless of this setting")
	inKafkaMdm.IntVar(&consumerConf.MaxWaitMs, "consumer-max-wait-ms", 100, "Maximum time the broker may wait to fill the response with fetch.min.bytes")
	inKafkaMdm.IntVar(&consumerConf.MetadataBackoffTime, "metadata-backoff-time", 500, "Time to wait between attempts to fetch metadata in ms")
	inKafkaMdm.IntVar(&consumerConf.MetadataRetries, "metadata-retries", 5, "Number of retries to fetch metadata in case of failure")
	inKafkaMdm.IntVar(&consumerConf.MetadataTimeout, "consumer-metadata-timeout-ms", 10000, "Maximum time to wait for the broker to reply to metadata queries in ms")
	inKafkaMdm.IntVar(&consumerConf.NetMaxOpenRequests, "net-max-open-requests", 100, "Maximum number of in-flight requests per broker connection. This is a generic property applied to all broker communication, however it is primarily relevant to produce requests.")
	inKafkaMdm.IntVar(&consumerConf.SessionTimeout, "consumer-session-timeout", 30000, "Client group session and failure detection timeout in ms")
	inKafkaMdm.StringVar(&consumerConf.Broker, "brokers", "kafka:9092", "tcp address for kafka (may be be given multiple times as a comma-separated list)")
	inKafkaMdm.StringVar(&consumerConf.StartAtOffset, "offset", "oldest", "Set the offset to start consuming from. Can be one of newest, oldest or a time duration")
	inKafkaMdm.StringVar(&consumerConf.Partitions, "partitions", "*", "kafka partitions to consume. use '*' or a comma separated list of id's")
	inKafkaMdm.StringVar(&topicStr, "topics", "mdm", "kafka topic (may be given multiple times as a comma-separated list)")
	globalconf.Register("kafka-mdm-in", inKafkaMdm)
}

func ConfigProcess(instance string) {
	if !Enabled {
		return
	}

	if consumerConf.OffsetCommitInterval == 0 {
		log.Fatal(4, "kafkamdm: offset-commit-interval must be greater then 0")
	}

	if consumerConf.MaxWaitMs == 0 {
		log.Fatal(4, "kafkamdm: consumer-max-wait-time must be greater then 0")
	}

	consumerConf.Topics = strings.Split(topicStr, ",")
	consumerConf.ClientID = instance + "-mdm"

	// record our partitions so others (MetricIdx) can use the partitioning information.
	// but only if the manager has been created (e.g. in metrictank), not when this input plugin is used in other contexts
	if cluster.Manager != nil {
		consumer, err := kafka.NewConsumer(consumerConf)
		if err != nil {
			log.Fatal(2, "kafka-cluster failed to initialize consumer: %s", err)
		}
		if LogLevel < 2 {
			log.Debug("kafkamdm: setting partitions on manager: %+v", consumer.Partitions)
		}
		cluster.Manager.SetPartitions(consumer.Partitions)
		consumer.Stop()
	}
}

func New() *KafkaMdm {
	log.Info("kafka-mdm consumer created without error")
	k := KafkaMdm{
		stopChan: make(chan struct{}),
	}

	consumerConf.GaugePrefix = "input.kafka-mdm.partition"
	consumerConf.MessageHandler = k.handleMsg

	var err error
	k.consumer, err = kafka.NewConsumer(consumerConf)
	if err != nil {
		log.Fatal(2, "kafka-cluster failed to initialize consumer: %s", err)
	}
	k.consumer.InitLagMonitor(10)

	return &k
}

func (k *KafkaMdm) Start(handler input.Handler, fatal chan struct{}) error {
	k.Handler = handler
	k.fatal = fatal

	return k.consumer.Start(nil)
}

func (k *KafkaMdm) handleMsg(data []byte, partition int32) {
	format, isPointMsg := msg.IsPointMsg(data)
	if isPointMsg {
		_, point, err := msg.ReadPointMsg(data, uint32(orgId))
		if err != nil {
			metricsDecodeErr.Inc()
			log.Error(3, "kafka-mdm decode error, skipping message. %s", err)
			return
		}
		k.Handler.ProcessMetricPoint(point, format, partition)
		return
	}

	md := schema.MetricData{}
	_, err := md.UnmarshalMsg(data)
	if err != nil {
		metricsDecodeErr.Inc()
		log.Error(3, "kafka-mdm decode error, skipping message. %s", err)
		return
	}
	metricsPerMessage.ValueUint32(1)
	k.Handler.ProcessMetricData(&md, partition)
}

func (k *KafkaMdm) Stop() {
	log.Info("kafka-mdm: stopping kafka input")
	close(k.stopChan)
	k.consumer.Stop()
}

func (k *KafkaMdm) MaintainPriority() {
	go func() {
		ticker := time.NewTicker(time.Second * 10)
		for {
			select {
			case <-k.stopChan:
				return
			case <-ticker.C:
				cluster.Manager.SetPriority(k.consumer.LagMonitor.Metric())
			}
		}
	}()
}
