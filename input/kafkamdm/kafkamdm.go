package kafkamdm

import (
	"flag"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/raintank/schema"
	"github.com/raintank/schema/msg"

	"github.com/Shopify/sarama"
	"github.com/rakyll/globalconf"
	log "github.com/sirupsen/logrus"

	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/input"
	"github.com/grafana/metrictank/kafka"
	"github.com/grafana/metrictank/stats"
)

// metric input.kafka-mdm.metrics_per_message is how many metrics per message were seen.
var metricsPerMessage = stats.NewMeter32("input.kafka-mdm.metrics_per_message", false)

// metric input.kafka-mdm.metrics_decode_err is a count of times an input message failed to parse
var metricsDecodeErr = stats.NewCounterRate32("input.kafka-mdm.metrics_decode_err")

type KafkaMdm struct {
	input.Handler
	consumer   sarama.Consumer
	client     sarama.Client
	lagMonitor *LagMonitor
	wg         sync.WaitGroup

	// signal to PartitionConsumers to shutdown
	stopConsuming chan struct{}
	// signal to caller that it should shutdown
	fatal chan struct{}
}

func (k *KafkaMdm) Name() string {
	return "kafka-mdm"
}

var LogLevel int
var Enabled bool
var orgId uint
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
	inKafkaMdm.UintVar(&orgId, "org-id", 0, "For incoming MetricPoint messages without org-id, assume this org id")
	inKafkaMdm.StringVar(&brokerStr, "brokers", "kafka:9092", "tcp address for kafka (may be be given multiple times as a comma-separated list)")
	inKafkaMdm.StringVar(&topicStr, "topics", "mdm", "kafka topic (may be given multiple times as a comma-separated list)")
	inKafkaMdm.StringVar(&offsetStr, "offset", "last", "Set the offset to start consuming from. Can be one of newest, oldest,last or a time duration")
	inKafkaMdm.StringVar(&partitionStr, "partitions", "*", "kafka partitions to consume. use '*' or a comma separated list of id's")
	inKafkaMdm.DurationVar(&offsetCommitInterval, "offset-commit-interval", time.Second*5, "Interval at which offsets should be saved.")
	inKafkaMdm.StringVar(&DataDir, "data-dir", "", "Directory to store partition offsets index")
	inKafkaMdm.IntVar(&channelBufferSize, "channel-buffer-size", 1000, "The number of metrics to buffer in internal and external channels")
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
		log.Fatal("kafkamdm: offset-commit-interval must be greater then 0")
	}
	if consumerMaxWaitTime == 0 {
		log.Fatal("kafkamdm: consumer-max-wait-time must be greater then 0")
	}
	if consumerMaxProcessingTime == 0 {
		log.Fatal("kafkamdm: consumer-max-processing-time must be greater then 0")
	}
	var err error
	switch offsetStr {
	case "last":
	case "oldest":
	case "newest":
	default:
		offsetDuration, err = time.ParseDuration(offsetStr)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err.Error(),
			}).Fatal("kafkamdm: invalid offset format")
		}
	}

	offsetMgr, err = kafka.NewOffsetMgr(DataDir)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Fatal("kafkamdm: couldn't create offsetMgr")
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
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Fatal("kafkamdm: invalid config")
	}
	// validate our partitions
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Fatal("kafkamdm: failed to create client")
	}
	defer client.Close()

	availParts, err := kafka.GetPartitions(client, topics)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Fatal("kafkamdm: GetPartitions error")
	}
	log.WithFields(log.Fields{
		"available.partitions": availParts,
	}).Info("kafkamdm: partitions available")
	if partitionStr == "*" {
		partitions = availParts
	} else {
		parts := strings.Split(partitionStr, ",")
		for _, part := range parts {
			i, err := strconv.Atoi(part)
			if err != nil {
				log.WithFields(log.Fields{
					"partition": part,
				}).Fatal("kafkamdm: could not parse partition, partitions must be '*' or a comma separated list of id's")
			}
			partitions = append(partitions, int32(i))
		}
		missing := kafka.DiffPartitions(partitions, availParts)
		if len(missing) > 0 {
			log.WithFields(log.Fields{
				"missing.partitions": missing,
			}).Fatal("kafkamdm: configured partitions not in list of available partitions")
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
		// metric input.kafka-mdm.partition.%d.offset is the current offset for the partition (%d) that we have consumed.
		partitionOffset[part] = stats.NewGauge64(fmt.Sprintf("input.kafka-mdm.partition.%d.offset", part))
		// metric input.kafka-mdm.partition.%d.log_size is the current size of the kafka partition (%d), aka the newest available offset.
		partitionLogSize[part] = stats.NewGauge64(fmt.Sprintf("input.kafka-mdm.partition.%d.log_size", part))
		// metric input.kafka-mdm.partition.%d.lag is how many messages (metrics) there are in the kafka partition (%d) that we have not yet consumed.
		partitionLag[part] = stats.NewGauge64(fmt.Sprintf("input.kafka-mdm.partition.%d.lag", part))
	}
}

func New() *KafkaMdm {
	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Fatal("kafkamdm: failed to create client")
	}
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Fatal("kafkamdm: failed to create consumer")
	}
	log.Info("kafkamdm: consumer created without error")
	k := KafkaMdm{
		consumer:      consumer,
		client:        client,
		lagMonitor:    NewLagMonitor(10, partitions),
		stopConsuming: make(chan struct{}),
	}

	return &k
}

func (k *KafkaMdm) Start(handler input.Handler, fatal chan struct{}) error {
	k.Handler = handler
	k.fatal = fatal
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
				if err != nil {
					log.WithFields(log.Fields{
						"offset":    offsetStr,
						"topic":     topic,
						"partition": partition,
						"error":     err.Error(),
					}).Error("kafkamdm: failed to get duration offset")
					return err
				}
			default:
				offset, err = k.client.GetOffset(topic, partition, time.Now().Add(-1*offsetDuration).UnixNano()/int64(time.Millisecond))
				if err != nil {
					offset = sarama.OffsetOldest
					log.WithFields(log.Fields{
						"offset.duration": offsetDuration,
						"error":           err.Error(),
					}).Warn("kafkamdm: failed to get offset, will use oldest instead")
				}
			}
			k.wg.Add(1)
			go k.consumePartition(topic, partition, offset)
		}
	}
	return nil
}

// tryGetOffset will to query kafka repeatedly for the requested offset and give up after attempts unsuccesfull attempts
// an error is returned when it had to give up
func (k *KafkaMdm) tryGetOffset(topic string, partition int32, offset int64, attempts int, sleep time.Duration) (int64, error) {

	var val int64
	var err error
	var offsetStr string

	switch offset {
	case sarama.OffsetNewest:
		offsetStr = "newest"
	case sarama.OffsetOldest:
		offsetStr = "oldest"
	default:
		offsetStr = strconv.FormatInt(offset, 10)
	}

	attempt := 1
	for {
		val, err = k.client.GetOffset(topic, partition, offset)
		if err == nil {
			break
		}

		err = fmt.Errorf("failed to get offset %s of partition %s:%d. %s (attempt %d/%d)", offsetStr, topic, partition, err, attempt, attempts)
		if attempt == attempts {
			break
		}
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Warn("kafkamdm: error")
		attempt += 1
		time.Sleep(sleep)
	}
	return val, err
}

// this will continually consume from the topic until k.stopConsuming is triggered.
func (k *KafkaMdm) consumePartition(topic string, partition int32, currentOffset int64) {
	defer k.wg.Done()

	partitionOffsetMetric := partitionOffset[partition]
	partitionLogSizeMetric := partitionLogSize[partition]
	partitionLagMetric := partitionLag[partition]

	// determine the pos of the topic and the initial offset of our consumer
	newest, err := k.tryGetOffset(topic, partition, sarama.OffsetNewest, 7, time.Second*10)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("kafkamdm: failed to get offset")
		close(k.fatal)
		return
	}
	if currentOffset == sarama.OffsetNewest {
		currentOffset = newest
	} else if currentOffset == sarama.OffsetOldest {
		currentOffset, err = k.tryGetOffset(topic, partition, sarama.OffsetOldest, 7, time.Second*10)
		if err != nil {
			log.WithFields(log.Fields{
				"error": err.Error(),
			}).Error("kafkamdm: failed to get offset")
			close(k.fatal)
			return
		}
	}

	partitionOffsetMetric.Set(int(currentOffset))
	partitionLogSizeMetric.Set(int(newest))
	partitionLagMetric.Set(int(newest - currentOffset))

	log.WithFields(log.Fields{
		"topic":          topic,
		"partition":      partition,
		"current.offset": currentOffset,
	}).Info("kafkamdm: consuming")
	pc, err := k.consumer.ConsumePartition(topic, partition, currentOffset)
	if err != nil {
		log.WithFields(log.Fields{
			"topic":     topic,
			"partition": partition,
			"error":     err.Error(),
		}).Error("kafkamdm: failed to start partitionConsumer")
		close(k.fatal)
		return
	}
	messages := pc.Messages()
	ticker := time.NewTicker(offsetCommitInterval)
	for {
		select {
		case msg, ok := <-messages:
			// https://github.com/Shopify/sarama/wiki/Frequently-Asked-Questions#why-am-i-getting-a-nil-message-from-the-sarama-consumer
			if !ok {
				log.WithFields(log.Fields{
					"topic":     topic,
					"partition": partition,
				}).Error("kafkamdm: kafka consumer has shutdown, stop consuming")
				if err := offsetMgr.Commit(topic, partition, currentOffset); err != nil {
					log.WithFields(log.Fields{
						"topic":     topic,
						"partition": partition,
						"error":     err.Error(),
					}).Error("kafkamdm: failed to commit offset")
				}
				close(k.fatal)
				return
			}
			log.WithFields(log.Fields{
				"topic":     msg.Topic,
				"partition": msg.Partition,
				"offset":    msg.Offset,
				"key":       msg.Key,
			}).Debug("kafkamdm: received message")
			k.handleMsg(msg.Value, partition)
			currentOffset = msg.Offset
		case ts := <-ticker.C:
			if err := offsetMgr.Commit(topic, partition, currentOffset); err != nil {
				log.WithFields(log.Fields{
					"topic":     topic,
					"partition": partition,
					"error":     err.Error(),
				}).Error("kafkamdm: failed to commit offset")
			}
			k.lagMonitor.StoreOffset(partition, currentOffset, ts)
			newest, err := k.tryGetOffset(topic, partition, sarama.OffsetNewest, 1, 0)
			if err != nil {
				log.WithFields(log.Fields{
					"error": err.Error(),
				}).Error("kafkamdm: failed to get offset")
			} else {
				partitionLogSizeMetric.Set(int(newest))
			}

			partitionOffsetMetric.Set(int(currentOffset))
			if err == nil {
				lag := int(newest - currentOffset)
				partitionLagMetric.Set(lag)
				k.lagMonitor.StoreLag(partition, lag)
			}
		case <-k.stopConsuming:
			pc.Close()
			if err := offsetMgr.Commit(topic, partition, currentOffset); err != nil {
				log.WithFields(log.Fields{
					"topic":     topic,
					"partition": partition,
					"error":     err.Error(),
				}).Error("kafkamdm: failed to commit offset")
			}
			log.WithFields(log.Fields{
				"topic":     topic,
				"partition": partition,
			}).Info("kafkamdm: consumer ended")
			return
		}
	}
}

func (k *KafkaMdm) handleMsg(data []byte, partition int32) {
	format, isPointMsg := msg.IsPointMsg(data)
	if isPointMsg {
		_, point, err := msg.ReadPointMsg(data, uint32(orgId))
		if err != nil {
			metricsDecodeErr.Inc()
			log.WithFields(log.Fields{
				"error": err.Error(),
			}).Error("kafkamdm: decode error, skipping message")
			return
		}
		k.Handler.ProcessMetricPoint(point, format, partition)
		return
	}

	md := schema.MetricData{}
	_, err := md.UnmarshalMsg(data)
	if err != nil {
		metricsDecodeErr.Inc()
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("kafkamdm: decode error, skipping message")
		return
	}
	metricsPerMessage.ValueUint32(1)
	k.Handler.ProcessMetricData(&md, partition)
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

func (k *KafkaMdm) MaintainPriority() {
	go func() {
		ticker := time.NewTicker(time.Second * 10)
		for {
			select {
			case <-k.stopConsuming:
				return
			case <-ticker.C:
				cluster.Manager.SetPriority(k.lagMonitor.Metric())
			}
		}
	}()
}

func (k *KafkaMdm) ExplainPriority() interface{} {
	return struct {
		Title       string
		Explanation interface{}
	}{
		"kafka-mdm:",
		k.lagMonitor.Explain(),
	}
}
