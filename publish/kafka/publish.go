package kafka

import (
	"errors"
	"flag"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/metrictank/conf"

	"github.com/Shopify/sarama"
	p "github.com/grafana/metrictank/cluster/partitioner"
	"github.com/grafana/metrictank/publish/kafka/keycache"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/schema/msg"
	"github.com/grafana/metrictank/stats"
	"github.com/grafana/metrictank/util"
	log "github.com/sirupsen/logrus"
)

var (
	producer        sarama.SyncProducer
	kafkaVersionStr string
	keyCache        *keycache.KeyCache

	schemasConf string

	publishedMD     = stats.NewCounterRate32("output.kafka.published.metricdata")
	publishedMP     = stats.NewCounterRate32("output.kafka.published.metricpoint")
	publishedMPNO   = stats.NewCounterRate32("output.kafka.published.metricpoint_no_org")
	messagesSize    = stats.NewMeter32("metrics.message_size", false)
	publishDuration = stats.NewLatencyHistogram15s32("metrics.publish")
	sendErrProducer = stats.NewCounterRate32("metrics.send_error.producer")
	sendErrOther    = stats.NewCounterRate32("metrics.send_error.other")

	topicsStr           string
	onlyOrgIds          util.Int64SliceFlag
	discardPrefixesStr  string
	codec               string
	enabled             bool
	partitionSchemesStr string
	maxMessages         int
	v2                  bool
	v2Org               bool
	v2ClearInterval     time.Duration
	flushFreq           time.Duration

	bufferPool   = util.NewBufferPool()
	bufferPool33 = util.NewBufferPool33()
)

type topicSettings struct {
	name            string
	partitioner     *p.Kafka
	numPartitions   int32
	onlyOrgId       int
	discardPrefixes []string
}

type mtPublisher struct {
	schemas      *conf.Schemas
	autoInterval bool
	topics       []topicSettings
}

type Partitioner interface {
	partition(schema.PartitionedMetric) (int32, []byte, error)
}

func init() {
	flag.StringVar(&topicsStr, "metrics-topic", "mdm", "topic for metrics (may be given multiple times as a comma-separated list)")
	flag.Var(&onlyOrgIds, "only-org-id", "restrict publishing data belonging to org id; 0 means no restriction (may be given multiple times, once per topic specified in 'metrics-topic', as a comma-separated list)")
	flag.StringVar(&discardPrefixesStr, "discard-prefixes", "", "discard data points starting with one of the given prefixes separated by | (may be given multiple times, once per topic specified in 'metrics-topic', as a comma-separated list)")
	flag.StringVar(&codec, "metrics-kafka-comp", "snappy", "compression: none|gzip|snappy")
	flag.BoolVar(&enabled, "metrics-publish", false, "enable metric publishing")
	flag.StringVar(&partitionSchemesStr, "metrics-partition-scheme", "bySeries", "method used for partitioning metrics. (byOrg|bySeries|bySeriesWithTags|bySeriesWithTagsFnv) (may be given multiple times, once per topic, as a comma-separated list)")
	flag.DurationVar(&flushFreq, "metrics-flush-freq", time.Millisecond*50, "The best-effort frequency of flushes to kafka")
	flag.IntVar(&maxMessages, "metrics-max-messages", 5000, "The maximum number of messages the producer will send in a single request")
	flag.StringVar(&schemasConf, "schemas-file", "/etc/metrictank/storage-schemas.conf", "path to carbon storage-schemas.conf file")
	flag.BoolVar(&v2, "v2", true, "enable optimized MetricPoint payload")
	flag.BoolVar(&v2Org, "v2-org", true, "encode org-id in messages")
	flag.DurationVar(&v2ClearInterval, "v2-clear-interval", time.Hour, "interval after which we always resend a full MetricData")
	flag.StringVar(&kafkaVersionStr, "kafka-version", "0.10.0.0", "Kafka version in semver format. All brokers must be this version or newer.")
}

func getCompression(codec string) sarama.CompressionCodec {
	switch codec {
	case "none":
		return sarama.CompressionNone
	case "gzip":
		return sarama.CompressionGZIP
	case "snappy":
		return sarama.CompressionSnappy
	default:
		log.Fatalf("unknown compression codec %q", codec)
		return 0 // make go compiler happy, needs a return *roll eyes*
	}
}

func parseTopicSettings(partitionSchemesStr, topicsStr string, onlyOrgIds []int64, discardPrefixesStr string) ([]topicSettings, error) {
	var topics []topicSettings
	partitionSchemes := strings.Split(partitionSchemesStr, ",")
	topicsStrList := strings.Split(topicsStr, ",")
	var discardPrefixesStrList []string
	if discardPrefixesStr != "" {
		discardPrefixesStrList = strings.Split(discardPrefixesStr, ",")
	}
	if len(partitionSchemes) > 1 && len(partitionSchemes) != len(topicsStrList) {
		return nil, errors.New("More partition schemes (metrics-partition-scheme) than topics (metrics-topic)")
	}
	if len(onlyOrgIds) > 1 && len(onlyOrgIds) != len(topicsStrList) {
		return nil, errors.New("More org ids (only-org-id) than topics (metrics-topic)")
	}
	if len(discardPrefixesStrList) > 0 && len(discardPrefixesStrList) != len(topicsStrList) {
		return nil, errors.New("More discard prefixes (discard-prefixes) than topics (metrics-topic)")
	}
	for i, topicName := range topicsStrList {
		topicName = strings.TrimSpace(topicName)
		var partitioner *p.Kafka
		if len(partitionSchemes) == 1 && i > 0 {
			// if only one partition scheme is specified, share first partitioner
			partitioner = topics[0].partitioner
		} else {
			var err error
			partitioner, err = p.NewKafka(strings.TrimSpace(partitionSchemes[i]))
			if err != nil {
				return nil, err
			}
		}
		var onlyOrgId int64
		if len(onlyOrgIds) == 0 {
			// if no only-org-id flag is specified, assume no restriction on org id
			onlyOrgId = 0
		} else if len(onlyOrgIds) == 1 {
			// if only one only-org-id flag is specified, use same restriction for all topics
			onlyOrgId = onlyOrgIds[0]
		} else {
			onlyOrgId = onlyOrgIds[i]
		}

		topic := topicSettings{
			name:        topicName,
			partitioner: partitioner,
			onlyOrgId:   int(onlyOrgId),
		}

		if len(discardPrefixesStrList) > 0 {
			// split prefixes by '|'; similar to strings.Split() but removes empty splits
			f := func(c rune) bool {
				return c == '|'
			}
			topic.discardPrefixes = strings.FieldsFunc(discardPrefixesStrList[i], f)
		}

		topics = append(topics, topic)
	}
	return topics, nil
}

func New(brokers []string, autoInterval bool) *mtPublisher {
	if !enabled {
		return nil
	}

	for _, b := range brokers {
		if b == "" {
			log.Fatal("invalid broker ''")
		}
		cnt := strings.Count(b, ":")
		if cnt > 1 {
			log.Fatalf("invalid broker %q", b)
		}
		if cnt == 1 {
			parts := strings.SplitN(b, ":", 2)
			if parts[0] == "" || parts[1] == "" {
				log.Fatalf("invalid broker %q", b)
			}
			if _, err := strconv.Atoi(parts[1]); err != nil {
				log.Fatalf("invalid broker %q: %s", b, err.Error())
			}
		}
	}

	kafkaVersion, err := sarama.ParseKafkaVersion(kafkaVersionStr)
	if err != nil {
		log.Fatalf("invalid kafka-version. %s", err)
	}

	mp := mtPublisher{
		autoInterval: autoInterval,
	}

	mp.topics, err = parseTopicSettings(partitionSchemesStr, topicsStr, onlyOrgIds, discardPrefixesStr)
	if err != nil {
		log.Fatalf("failed to initialize partitioner: %s", err)
	}

	if autoInterval {
		schemas, err := conf.ReadSchemas(schemasConf)
		if err != nil {
			log.Fatalf("failed to load schemas config. %s", err)
		}
		mp.schemas = &schemas
	}

	// We are looking for strong consistency semantics.
	// Because we don't change the flush settings, sarama will try to produce messages
	// as fast as possible to keep latency low.
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Compression = getCompression(codec)
	config.Producer.Return.Successes = true
	config.Producer.Flush.Frequency = flushFreq
	config.Producer.Flush.MaxMessages = maxMessages
	config.Producer.Partitioner = sarama.NewManualPartitioner
	config.Version = kafkaVersion
	err = config.Validate()
	if err != nil {
		log.Fatalf("failed to validate kafka config. %s", err)
	}

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		log.Fatalf("failed to initialize kafka client %s", err)
	}

	for i, setting := range mp.topics {
		partitions, err := client.Partitions(setting.name)
		if err != nil {
			log.Fatalf("failed to get number of partitions %s", err)
		}
		if len(partitions) < 1 {
			log.Fatalf("failed to get number of partitions for topic %s", setting.name)
		}
		mp.topics[i].numPartitions = int32(len(partitions))
	}

	producer, err = sarama.NewSyncProducerFromClient(client)
	if err != nil {
		log.Fatalf("failed to initialize kafka producer. %s", err)
	}

	if v2 {
		keyCache = keycache.NewKeyCache(v2ClearInterval)
	}

	return &mp
}

func (m *mtPublisher) Publish(metrics []*schema.MetricData) error {
	if producer == nil {
		log.Debugf("dropping %d metrics as publishing is disabled", len(metrics))
		return nil
	}

	if len(metrics) == 0 {
		return nil
	}

	if len(m.topics) == 0 {
		return nil
	}

	var err error

	metricsCount := len(metrics)
	// plan for a maximum of metrics*topics messages to be sent
	payload := make([]*sarama.ProducerMessage, 0, metricsCount*len(m.topics))
	pre := time.Now()
	pubMD := make(map[string]int)
	pubMP := make(map[string]int)
	pubMPNO := make(map[string]int)
	buffersToRelease := [][]byte{}

	for _, metric := range metrics {
		if metric.Interval == 0 {
			if m.autoInterval {
				_, s := m.schemas.Match(metric.Name, 0)
				metric.Interval = s.Retentions.Rets[0].SecondsPerPoint
			} else {
				log.Error("interval is 0 but can't deduce interval automatically. this should never happen")
				return errors.New("need to deduce interval but cannot")
			}
		}
		metric.SetId()


		isMD := false
		isMP := false
		isMPNO := false

		var data []byte
		if v2 {
			var mkey schema.MKey
			mkey, err = schema.MKeyFromString(metric.Id)
			if err != nil {
				return err
			}
			ok := keyCache.Touch(mkey)
			// we've seen this key recently. we can use the optimized format
			if ok {
				data = bufferPool33.Get()
				mp := schema.MetricPoint{
					MKey:  mkey,
					Value: metric.Value,
					Time:  uint32(metric.Time),
				}
				if v2Org {
					data = data[:33]                      // this range will contain valid data
					data[0] = byte(msg.FormatMetricPoint) // store version in first byte
					_, err = mp.Marshal32(data[:1])       // Marshal will fill up space between length and cap, i.e. bytes 2-33
					isMP = true
				} else {
					data = data[:29]                                // this range will contain valid data
					data[0] = byte(msg.FormatMetricPointWithoutOrg) // store version in first byte
					_, err = mp.MarshalWithoutOrg28(data[:1])       // Marshal will fill up space between length and cap, i.e. bytes 2-29
					isMPNO = true
				}
			} else {
				data = bufferPool.Get()
				data, err = metric.MarshalMsg(data)
				if err != nil {
					return err
				}
				isMD = true
			}
		} else {
			data = bufferPool.Get()
			data, err = metric.MarshalMsg(data)
			if err != nil {
				return err
			}
			isMD = true
		}

		buffersToRelease = append(buffersToRelease, data)

		for _, topic := range m.topics {
			partition, err := topic.partitioner.Partition(metric, topic.numPartitions)
			if err != nil {
				return err
			}

			prefixDiscarded := false
			for _, prefix := range topic.discardPrefixes {
				if strings.HasPrefix(metric.Name, prefix) {
					prefixDiscarded = true
					break
				}
			}

			if (topic.onlyOrgId == 0 || metric.OrgId == topic.onlyOrgId) &&
				!prefixDiscarded {
				message := &sarama.ProducerMessage{
					Partition: partition,
					Topic:     topic.name,
					Value:     sarama.ByteEncoder(data),
				}
				payload = append(payload, message)
				if isMP {
					pubMP[topic.name]++
				} else if isMD {
					pubMD[topic.name]++
				} else if isMPNO {
					pubMPNO[topic.name]++
				}
			}
		}

		messagesSize.Value(len(data))
	}

	defer func() {
		for _, buf := range buffersToRelease {
			if cap(buf) == 33 {
				bufferPool33.Put(buf)
			} else {
				bufferPool.Put(buf)
			}
		}
	}()

	err = producer.SendMessages(payload)
	if err != nil {
		if errors, ok := err.(sarama.ProducerErrors); ok {
			sendErrProducer.Add(len(errors))
			for i := 0; i < 10 && i < len(errors); i++ {
				log.Errorf("SendMessages ProducerError %d/%d: %s", i, len(errors), errors[i].Error())
			}
		} else {
			sendErrOther.Inc()
			log.Errorf("SendMessages error: %s", err.Error())
		}
		return err
	}

	publishDuration.Value(time.Since(pre))
	for _, topic := range m.topics {
		pubTopicMD := pubMD[topic.name]
		pubTopicMP := pubMP[topic.name]
		pubTopicMPNO := pubMPNO[topic.name]
		publishedMD.Add(pubTopicMD)
		publishedMP.Add(pubTopicMP)
		publishedMPNO.Add(pubTopicMPNO)
		log.Debugf("published %d metrics to topic %s", pubTopicMD+pubTopicMP+pubTopicMPNO, topic.name)
	}
	return nil
}

func (*mtPublisher) Type() string {
	return "Metrictank"
}
