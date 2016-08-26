package kafkamdm

import (
	"bytes"
	"encoding/binary"
	"flag"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"

	"github.com/raintank/met"
	"github.com/raintank/metrictank/idx"
	"github.com/raintank/metrictank/in"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/usage"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"github.com/syndtr/goleveldb/leveldb/storage"
)

type KafkaMdm struct {
	in.In
	consumer sarama.Consumer
	client   sarama.Client
	stats    met.Backend

	wg sync.WaitGroup
	// read from this channel to block until consumer is cleanly stopped
	StopChan chan int

	// signal to PartitionConsumers to shutdown
	stopConsuming chan struct{}
}

var LogLevel int
var Enabled bool
var brokerStr string
var brokers []string
var topicStr string
var topics []string
var offset string
var dataDir string
var config *sarama.Config
var channelBufferSize int
var consumerFetchMin int
var consumerFetchDefault int
var consumerMaxWaitTime time.Duration
var consumerMaxProcessingTime time.Duration
var netMaxOpenRequests int
var offsetMgr *OffsetMgr
var offsetDuration time.Duration
var offsetCommitInterval time.Duration

func ConfigSetup() {
	inKafkaMdm := flag.NewFlagSet("kafka-mdm-in", flag.ExitOnError)
	inKafkaMdm.BoolVar(&Enabled, "enabled", false, "")
	inKafkaMdm.StringVar(&brokerStr, "brokers", "kafka:9092", "tcp address for kafka (may be be given multiple times as a comma-separated list)")
	inKafkaMdm.StringVar(&topicStr, "topics", "mdm", "kafka topic (may be given multiple times as a comma-separated list)")
	inKafkaMdm.StringVar(&offset, "offset", "last", "Set the offset to start consuming from. Can be one of newest, oldest,last or a time duration")
	inKafkaMdm.DurationVar(&offsetCommitInterval, "offset-commit-interval", time.Second*5, "Interval at which offsets should be saved.")
	inKafkaMdm.StringVar(&dataDir, "data-dir", "", "Directory to store partition offsets index")
	inKafkaMdm.IntVar(&channelBufferSize, "channel-buffer-size", 1000000, "The number of metrics to buffer in internal and external channels")
	inKafkaMdm.IntVar(&consumerFetchMin, "consumer-fetch-min", 1024000, "The minimum number of message bytes to fetch in a request")
	inKafkaMdm.IntVar(&consumerFetchDefault, "consumer-fetch-default", 4096000, "The default number of message bytes to fetch in a request")
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

	switch offset {
	case "last":
	case "oldest":
	case "newest":
	default:
		offsetDuration, err = time.ParseDuration(offset)
		if err != nil {
			log.Fatal(4, "kafkamdm: invalid offest format. %s", err)
		}
	}

	var err error
	offsetMgr, err = NewOffsetMgr(dataDir)
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
}

func New(stats met.Backend) *KafkaMdm {
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
		stats:         stats,
		StopChan:      make(chan int),
		stopConsuming: make(chan struct{}),
	}

	return &k
}

func (k *KafkaMdm) Start(metrics mdata.Metrics, metricIndex idx.MetricIndex, usg *usage.Usage) {
	k.In = in.New(metrics, metricIndex, usg, "kafka-mdm", k.stats)
	for _, topic := range topics {
		// get partitions.
		partitions, err := k.consumer.Partitions(topic)
		if err != nil {
			log.Fatal(4, "kafka-mdm: Faild to get partitions for topic %s. %s", topic, err)
		}
		for _, partition := range partitions {
			switch offset {
			case "oldest":
				go k.consumePartition(topic, partition, -2)
			case "newest":
				go k.consumePartition(topic, partition, -1)
			case "last":
				o, err := offsetMgr.Last(topic, partition)
				if err != nil {
					log.Fatal(4, "kafka-mdm: Failed to get offset for %s:%d. %s", topic, partition, err)
				}
				go k.consumePartition(topic, partition, o)
			default:
				o, err := k.client.GetOffset(topic, partition, time.Now().Add(-1*offsetDuration).UnixNano()/int64(time.Millisecond))
				if err != nil {
					log.Fatal(4, "kafka-mdm: failed to get offset for %s:%d.  %s", topic, partition, err)
				}
				go k.consumePartition(topic, partition, o)
			}
		}
	}
}

// this will continually consume from the topic until k.stopConsuming is triggered.
func (k *KafkaMdm) consumePartition(topic string, partition int32, partitionOffset int64) {
	k.wg.Add(1)
	defer k.wg.Done()

	pc, err := k.consumer.ConsumePartition(topic, partition, partitionOffset)
	if err != nil {
		log.Fatal(4, "kafka-mdm: failed to start partitionConsumer for %s:%d. %s", topic, partition, err)
	}
	log.Info("kafka-mdm: consuming from %s:%d from offset %d", topic, partition, partitionOffset)
	currentOffset := partitionOffset
	messages := pc.Messages()
	ticker := time.NewTicker(offsetCommitInterval)
	for {
		select {
		case msg := <-messages:
			if LogLevel < 2 {
				log.Debug("kafka-mdm received message: Topic %s, Partition: %d, Offset: %d, Key: %x", msg.Topic, msg.Partition, msg.Offset, msg.Key)
			}
			k.In.Handle(msg.Value)
			currentOffset = msg.Offset
		case <-ticker.C:
			if err := offsetMgr.Commit(topic, partition, currentOffset); err != nil {
				log.Error(3, "kafka-mdm failed to commit offset for %s:%d, %s", topic, partition, err)
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

// Stop will initiate a graceful stop of the Consumer (permanent)
//
// NOTE: receive on StopChan to block until this process completes
func (k *KafkaMdm) Stop() {
	// closes notifications and messages channels, amongst others
	close(k.stopConsuming)
	go func() {
		k.wg.Wait()
		offsetMgr.Close()
		close(k.StopChan)
	}()
}

type OffsetMgr struct {
	db *leveldb.DB
}

func NewOffsetMgr(dir string) (*OffsetMgr, error) {
	dbFile := filepath.Join(dir, "partitionOffsets.db")
	db, err := leveldb.OpenFile(dbFile, &opt.Options{})
	if err != nil {
		if _, ok := err.(*storage.ErrCorrupted); ok {
			log.Warn("partitionOffsets.db is corrupt. Recovering.")
			db, err = leveldb.RecoverFile(dbFile, &opt.Options{})
			if err != nil {
				return nil, err
			}
		} else {
			return nil, err
		}
	}
	log.Info("Opened %s", dbFile)
	return &OffsetMgr{
		db: db,
	}, nil
}

func (o *OffsetMgr) Close() {
	log.Info("Closing partitionsOffset DB.")
	o.db.Close()
}

func (o *OffsetMgr) Commit(topic string, partition int32, offset int64) error {
	key := new(bytes.Buffer)
	key.WriteString(fmt.Sprintf("T:%s-P:%d", topic, partition))
	data := new(bytes.Buffer)
	if err := binary.Write(data, binary.LittleEndian, offset); err != nil {
		return err
	}
	log.Debug("commiting offset %d for %s:%d to partitionsOffset.db", offset, topic, partition)
	return o.db.Put(key.Bytes(), data.Bytes(), &opt.WriteOptions{Sync: true})
}

func (o *OffsetMgr) Last(topic string, partition int32) (int64, error) {
	key := new(bytes.Buffer)
	key.WriteString(fmt.Sprintf("T:%s-P:%d", topic, partition))
	data, err := o.db.Get(key.Bytes(), nil)
	if err != nil {
		if err == leveldb.ErrNotFound {
			log.Debug("no offset recorded for %s:%d", topic, partition)
			return -1, nil
		}
		return 0, err
	}
	var offset int64
	err = binary.Read(bytes.NewBuffer(data), binary.LittleEndian, &offset)
	if err != nil {
		return 0, err
	}
	log.Debug("found saved offset %d for %s:%d", offset, topic, partition)
	return offset, nil
}
