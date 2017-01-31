package kafkamdm

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"

	"github.com/raintank/metrictank/cluster"
	"github.com/raintank/metrictank/input"
	"github.com/raintank/metrictank/kafka"
	"github.com/raintank/metrictank/stats"
	"gopkg.in/raintank/schema.v1"
)

// metric input.kafka-mdm.metrics_per_message is how many metrics per message were seen.
var metricsPerMessage = stats.NewMeter32("input.kafka-mdm.metrics_per_message", false)

// metric input.kafka-mdm.metrics_decode_err is a count of times an input message failed to parse
var metricsDecodeErr = stats.NewCounter32("input.kafka-mdm.metrics_decode_err")

type KafkaMdm struct {
	input.Handler
	consumer sarama.Consumer
	client   sarama.Client

	wg sync.WaitGroup

	// signal to PartitionConsumers to shutdown
	stopConsuming chan struct{}
}

func (k *KafkaMdm) Name() string {
	return "kafka-mdm"
}

var LogLevel int
var Enabled bool
var brokerStr string
var brokers []string
var topicStr string
var topics []string
var partitionStr string
var partitions []int32
var offsetStr string
var DataDir string
var config *sarama.Config
var channelBufferSize int
var consumerFetchMin int
var consumerFetchDefault int
var consumerMaxWaitTime time.Duration
var consumerMaxProcessingTime time.Duration
var netMaxOpenRequests int
var offsetMgr *kafka.OffsetMgr
var offsetDuration time.Duration
var offsetCommitInterval time.Duration
var partitionOffset map[int32]*stats.Gauge64
var partitionLogSize map[int32]*stats.Gauge64
var partitionLag map[int32]*stats.Gauge64

func ConfigSetup() {
	inKafkaMdm := flag.NewFlagSet("kafka-mdm-in", flag.ExitOnError)
	inKafkaMdm.BoolVar(&Enabled, "enabled", false, "")
	inKafkaMdm.StringVar(&brokerStr, "brokers", "kafka:9092", "tcp address for kafka (may be be given multiple times as a comma-separated list)")
	inKafkaMdm.StringVar(&topicStr, "topics", "mdm", "kafka topic (may be given multiple times as a comma-separated list)")
	inKafkaMdm.StringVar(&offsetStr, "offset", "last", "Set the offset to start consuming from. Can be one of newest, oldest,last or a time duration")
	inKafkaMdm.StringVar(&partitionStr, "partitions", "*", "kafka partitions to consume. use '*' or a comma separated list of id's")
	inKafkaMdm.DurationVar(&offsetCommitInterval, "offset-commit-interval", time.Second*5, "Interval at which offsets should be saved.")
	inKafkaMdm.StringVar(&DataDir, "data-dir", "", "Directory to store partition offsets index")
	inKafkaMdm.IntVar(&channelBufferSize, "channel-buffer-size", 1000000, "The number of metrics to buffer in internal and external channels")
	inKafkaMdm.IntVar(&consumerFetchMin, "consumer-fetch-min", 1, "The minimum number of message bytes to fetch in a request")
	inKafkaMdm.IntVar(&consumerFetchDefault, "consumer-fetch-default", 32768, "The default number of message bytes to fetch in a request")
	inKafkaMdm.DurationVar(&consumerMaxWaitTime, "consumer-max-wait-time", time.Second, "The maximum amount of time the broker will wait for Consumer.Fetch.Min bytes to become available before it returns fewer than that anyway")
	inKafkaMdm.DurationVar(&consumerMaxProcessingTime, "consumer-max-processing-time", time.Second, "The maximum amount of time the consumer expects a message takes to process")
	inKafkaMdm.IntVar(&netMaxOpenRequests, "net-max-open-requests", 100, "How many outstanding requests a connection is allowed to have before sending on it blocks")
	globalconf.Register("kafka-mdm-in", inKafkaMdm)
}

func ConfigProcess(instance string) {
	if !Enabled {
		return
	}

	if offsetCommitInterval == 0 {
		log.Fatal(4, "kafkamdm: offset-commit-interval must be greater then 0")
	}
	if consumerMaxWaitTime == 0 {
		log.Fatal(4, "kafkamdm: consumer-max-wait-time must be greater then 0")
	}
	if consumerMaxProcessingTime == 0 {
		log.Fatal(4, "kafkamdm: consumer-max-processing-time must be greater then 0")
	}
	var err error
	switch offsetStr {
	case "last":
	case "oldest":
	case "newest":
	default:
		offsetDuration, err = time.ParseDuration(offsetStr)
		if err != nil {
			log.Fatal(4, "kafkamdm: invalid offest format. %s", err)
		}
	}

	offsetMgr, err = kafka.NewOffsetMgr(DataDir)
	if err != nil {
		log.Fatal(4, "kafka-mdm couldnt create offsetMgr. %s", err)
	}
	brokers = strings.Split(brokerStr, ",")
	topics = strings.Split(topicStr, ",")

	config = sarama.NewConfig()

	config.ClientID = instance + "-mdm"
	config.ChannelBufferSize = channelBufferSize
	config.Consumer.Fetch.Min = int32(consumerFetchMin)
	config.Consumer.Fetch.Default = int32(consumerFetchDefault)
	config.Consumer.MaxWaitTime = consumerMaxWaitTime
	config.Consumer.MaxProcessingTime = consumerMaxProcessingTime
	config.Net.MaxOpenRequests = netMaxOpenRequests
	config.Version = sarama.V0_10_0_0
	err = config.Validate()
	if err != nil {
		log.Fatal(2, "kafka-mdm invalid config: %s", err)
	}
	// validate our partitions
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		log.Fatal(4, "kafka-mdm failed to create client. %s", err)
	}
	defer client.Close()

	availParts, err := kafka.GetPartitions(client, topics)
	if err != nil {
		log.Fatal(4, "kafka-mdm: %s", err.Error())
	}
	log.Info("kafka-mdm: available partitions %v", availParts)
	if partitionStr == "*" {
		partitions = availParts
	} else {
		parts := strings.Split(partitionStr, ",")
		for _, part := range parts {
			i, err := strconv.Atoi(part)
			if err != nil {
				log.Fatal(4, "could not parse partition %q. partitions must be '*' or a comma separated list of id's", part)
			}
			partitions = append(partitions, int32(i))
		}
		missing := kafka.DiffPartitions(partitions, availParts)
		if len(missing) > 0 {
			log.Fatal(4, "kafka-mdm: configured partitions not in list of available partitions. missing %v", missing)
		}
	}
	// record our partitions so others (MetricIdx) can use the partitioning information.
	// but only if the manager has been created (e.g. in metrictank), not when this input plugin is used in other contexts
	if cluster.Manager != nil {
		cluster.Manager.SetPartitions(partitions)
	}

	// initialize our offset metrics
	partitionOffset = make(map[int32]*stats.Gauge64)
	partitionLogSize = make(map[int32]*stats.Gauge64)
	partitionLag = make(map[int32]*stats.Gauge64)
	for _, part := range partitions {
		partitionOffset[part] = stats.NewGauge64(fmt.Sprintf("input.kafka-mdm.partition.%d.offset", part))
		partitionLogSize[part] = stats.NewGauge64(fmt.Sprintf("input.kafka-mdm.partition.%d.log_size", part))
		partitionLag[part] = stats.NewGauge64(fmt.Sprintf("input.kafka-mdm.partition.%d.lag", part))
	}
}

func New() *KafkaMdm {
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		log.Fatal(4, "kafka-mdm failed to create client. %s", err)
	}
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.Fatal(2, "kafka-mdm failed to create consumer: %s", err)
	}
	log.Info("kafka-mdm consumer created without error")
	k := KafkaMdm{
		consumer:      consumer,
		client:        client,
		stopConsuming: make(chan struct{}),
	}

	return &k
}

func (k *KafkaMdm) Start(handler input.Handler) {
	k.Handler = handler
	var err error
	for _, topic := range topics {
		for _, partition := range partitions {
			var offset int64
			switch offsetStr {
			case "oldest":
				offset = sarama.OffsetOldest
			case "newest":
				offset = sarama.OffsetNewest
			case "last":
				offset, err = offsetMgr.Last(topic, partition)
			default:
				offset, err = k.client.GetOffset(topic, partition, time.Now().Add(-1*offsetDuration).UnixNano()/int64(time.Millisecond))
			}
			if err != nil {
				log.Fatal(4, "kafka-mdm: Failed to get %q duration offset for %s:%d. %q", offsetStr, topic, partition, err)
			}
			go k.consumePartition(topic, partition, offset)
		}
	}
}

// this will continually consume from the topic until k.stopConsuming is triggered.
func (k *KafkaMdm) consumePartition(topic string, partition int32, currentOffset int64) {
	k.wg.Add(1)
	defer k.wg.Done()

	pc, err := k.consumer.ConsumePartition(topic, partition, currentOffset)
	if err != nil {
		log.Fatal(4, "kafka-mdm: failed to start partitionConsumer for %s:%d. %s", topic, partition, err)
	}

	partitionOffsetMetric := partitionOffset[partition]
	partitionLogSizeMetric := partitionLogSize[partition]
	partitionLagMetric := partitionLag[partition]
	offset, err := k.client.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		log.Error(3, "kafka-mdm failed to get log-size of partition %s:%d. %s", topic, partition, err)
	} else {
		partitionLogSizeMetric.Set(int(offset))
	}
	if currentOffset >= 0 {
		// we cant set the offsetMetrics until we know what offset we are at.
		partitionOffsetMetric.Set(int(currentOffset))
		// we need the currentLogSize to be able to record our inital Lag.
		if err == nil {
			partitionLagMetric.Set(int(offset - currentOffset))
		}
	}

	log.Info("kafka-mdm: consuming from %s:%d from offset %d", topic, partition, currentOffset)
	messages := pc.Messages()
	ticker := time.NewTicker(offsetCommitInterval)

	for {
		select {
		case msg := <-messages:
			if LogLevel < 2 {
				log.Debug("kafka-mdm received message: Topic %s, Partition: %d, Offset: %d, Key: %x", msg.Topic, msg.Partition, msg.Offset, msg.Key)
			}
			k.handleMsg(msg.Value, partition)
			currentOffset = msg.Offset
		case <-ticker.C:
			if err := offsetMgr.Commit(topic, partition, currentOffset); err != nil {
				log.Error(3, "kafka-mdm failed to commit offset for %s:%d, %s", topic, partition, err)
			}
			offset, err := k.client.GetOffset(topic, partition, sarama.OffsetNewest)
			if err != nil {
				log.Error(3, "kafka-mdm failed to get log-size of partition %s:%d. %s", topic, partition, err)
			} else {
				partitionLogSizeMetric.Set(int(offset))
			}
			if currentOffset < 0 {
				// we have not yet consumed any messages.
				continue
			}
			partitionOffsetMetric.Set(int(currentOffset))
			if err == nil {
				partitionLagMetric.Set(int(offset - currentOffset))
			}
		case <-k.stopConsuming:
			pc.Close()
			if err := offsetMgr.Commit(topic, partition, currentOffset); err != nil {
				log.Error(3, "kafka-mdm failed to commit offset for %s:%d, %s", topic, partition, err)
			}
			log.Info("kafka-mdm consumer for %s:%d ended.", topic, partition)
			return
		}
	}
}

func (k *KafkaMdm) handleMsg(data []byte, partition int32) {
	md := schema.MetricData{}
	_, err := md.UnmarshalMsg(data)
	if err != nil {
		metricsDecodeErr.Inc()
		log.Error(3, "kafka-mdm decode error, skipping message. %s", err)
		return
	}
	metricsPerMessage.ValueUint32(1)
	k.Handler.Process(&md, partition)
}

// Stop will initiate a graceful stop of the Consumer (permanent)
// and block until it stopped.
func (k *KafkaMdm) Stop() {
	// closes notifications and messages channels, amongst others
	close(k.stopConsuming)
	k.wg.Wait()
	k.client.Close()
	offsetMgr.Close()
}
