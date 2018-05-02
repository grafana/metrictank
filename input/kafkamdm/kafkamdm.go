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

var clientConf *kafka.ClientConf
var Enabled bool
var orgId uint
var LogLevel int
var topicStr string

func ConfigSetup() {
	clientConf = kafka.NewConfig()
	inKafkaMdm := flag.NewFlagSet("kafka-mdm-in", flag.ExitOnError)
	inKafkaMdm.BoolVar(&Enabled, "enabled", false, "")
	inKafkaMdm.UintVar(&orgId, "org-id", 0, "For incoming MetricPoint messages without org-id, assume this org id")
	inKafkaMdm.DurationVar(&clientConf.LagCollectionInterval, "lag-collection-interval", time.Second*5, "Interval at which the lag is calculated and saved")
	inKafkaMdm.IntVar(&clientConf.ConsumerThreads, "consumer-threads", 4, "Number of kafka client threads")
	inKafkaMdm.IntVar(&clientConf.EventsChannelSize, "events-channel-size", 50, "Size of the channel that enqueues incoming events")
	inKafkaMdm.IntVar(&clientConf.FetchMin, "fetch-min", 1, "Minimum number of bytes the broker responds with. If fetch.wait.max.ms expires the accumulated data will be sent to the client regardless of this setting")
	inKafkaMdm.IntVar(&clientConf.FetchMessageMax, "fetch-message-max", 32768, "Initial maximum number of bytes per topic+partition to request when fetching messages from the broker.")
	inKafkaMdm.DurationVar(&clientConf.MaxWait, "max-wait", time.Millisecond*100, "Maximum time the broker may wait to fill the response with fetch.min.bytes")
	inKafkaMdm.DurationVar(&clientConf.MetadataBackoffTime, "metadata-backoff-time", time.Millisecond*500, "Time to wait between attempts to fetch metadata")
	inKafkaMdm.IntVar(&clientConf.MetadataRetries, "metadata-retries", 5, "Number of retries to fetch metadata in case of failure")
	inKafkaMdm.DurationVar(&clientConf.MetadataTimeout, "metadata-timeout", time.Second*10, "Maximum time to wait for the broker to reply to metadata queries")
	inKafkaMdm.IntVar(&clientConf.NetMaxOpenRequests, "net-max-open-requests", 100, "Maximum number of in-flight requests per broker connection. This is a generic property applied to all broker communication, however it is primarily relevant to produce requests.")
	inKafkaMdm.DurationVar(&clientConf.SessionTimeout, "session-timeout", time.Second*30, "Client group session and failure detection timeout")
	inKafkaMdm.StringVar(&clientConf.Broker, "brokers", "kafka:9092", "tcp address for kafka (may be be given multiple times as a comma-separated list)")
	inKafkaMdm.StringVar(&clientConf.StartAtOffset, "offset", "oldest", "Set the offset to start consuming from. Can be one of newest, oldest or a time duration")
	inKafkaMdm.StringVar(&clientConf.Partitions, "partitions", "*", "kafka partitions to consume. use '*' or a comma separated list of id's")
	inKafkaMdm.StringVar(&topicStr, "topics", "mdm", "kafka topic (may be given multiple times as a comma-separated list)")
	globalconf.Register("kafka-mdm-in", inKafkaMdm)
}

func ConfigProcess(instance string) {
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

	clientConf.Topics = strings.Split(topicStr, ",")
	clientConf.ClientID = instance + "-mdm"

	// record our partitions so others (MetricIdx) can use the partitioning information.
	// but only if the manager has been created (e.g. in metrictank), not when this input plugin is used in other contexts
	if cluster.Manager != nil {
		consumer, err := kafka.NewConsumer(clientConf)
		if err != nil {
			log.Fatal(2, "kafka-cluster failed to initialize consumer: %s", err)
		}
		log.Debug("kafkamdm: setting partitions on manager: %+v", consumer.Partitions)
		cluster.Manager.SetPartitions(consumer.Partitions)
		consumer.Stop()
	}
}

func New() *KafkaMdm {
	log.Info("kafka-mdm consumer created without error")
	k := KafkaMdm{
		stopChan: make(chan struct{}),
	}

	clientConf.GaugePrefix = "input.kafka-mdm.partition"
	clientConf.MessageHandler = k.handleMsg

	var err error
	k.consumer, err = kafka.NewConsumer(clientConf)
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
				metric := k.consumer.LagMonitor.Metric()
				cluster.Manager.SetPriority(metric)
			}
		}
	}()
}
