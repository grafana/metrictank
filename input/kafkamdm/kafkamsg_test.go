package kafkamdm

import (
	"context"
	"crypto/md5"
	"fmt"
	"os"
	"strconv"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/raintank/schema"
	"github.com/raintank/schema/msg"
	"github.com/raintank/tsdb-gw/util"
	log "github.com/sirupsen/logrus"
)

var keys []string

type benchRun struct {
	*testing.B

	config   *sarama.Config
	producer sarama.SyncProducer
	pc       sarama.PartitionConsumer

	topic       string
	partition   int32
	batchSize   int
	pointPerMsg int

	pointsRecvd int

	bufferPool   *util.BufferPool
	bufferPool33 *util.BufferPool33
}

func newBenchRun(b *testing.B) *benchRun {
	bench := &benchRun{
		B:            b,
		topic:        "test",
		partition:    0,
		batchSize:    5000,
		pointPerMsg:  100,
		bufferPool:   util.NewBufferPool(),
		bufferPool33: util.NewBufferPool33(),
	}
	config := sarama.NewConfig()
	config.ClientID = "benchmark"
	config.ChannelBufferSize = 1000
	config.Consumer.Fetch.Min = int32(1)
	config.Consumer.Fetch.Default = int32(32768)
	config.Consumer.MaxWaitTime = time.Second
	config.Consumer.MaxProcessingTime = time.Second
	config.Producer.RequiredAcks = sarama.WaitForAll // Wait for all in-sync replicas to ack the message
	config.Producer.Retry.Max = 10                   // Retry up to 10 times to produce the message
	config.Producer.Compression = sarama.CompressionSnappy
	config.Producer.Return.Successes = true
	config.Producer.Flush.Frequency = time.Millisecond * 50
	config.Producer.Flush.MaxMessages = 5000
	config.Net.MaxOpenRequests = 100
	config.Version = sarama.V0_10_0_0

	err := config.Validate()
	if err != nil {
		log.Fatalf("kafka-mdm invalid config: %s", err)
	}
	bench.config = config
	log.Info("producer starting")
	producer, err := sarama.NewSyncProducer([]string{"localhost:9092"}, config)
	if err != nil {
		log.Fatalf("failed to initialize kafka producer. %s", err)
	}
	bench.producer = producer
	return bench
}

func init() {
	// Output to stdout instead of the default stderr
	// Can be any io.Writer, see below for File example
	log.SetOutput(os.Stdout)

	// Only log the info severity or above.
	log.SetLevel(log.ErrorLevel)
}

// benchmark processing 1 MetricPoint per kafka message.
// Snappy compression is used for each kafka message.
func Benchmark1PointPerMessageSnappy(b *testing.B) {
	bench := newBenchRun(b)
	bench.topic = "bench1snappy"
	bench.run(false)
}

// benchmark processing 10 metricPoints per kafka message.
// Snappy compression is used for each kafka message.
func Benchmark10PointPerMessageSnappy(b *testing.B) {
	bench := newBenchRun(b)
	bench.topic = "bench10snappy"
	bench.pointPerMsg = 10
	bench.run(true)
}

// benchmark processing 50 metricPoints per kafka message.
// Snappy compression is used for each kafka message.
func Benchmark50PointPerMessageSnappy(b *testing.B) {
	bench := newBenchRun(b)
	bench.topic = "bench50snappy"
	bench.pointPerMsg = 50
	bench.run(true)
}

// benchmark processing 100 metricPoints per kafka message.
// Snappy compression is used for each kafka message.
func Benchmark100PointPerMessageSnappy(b *testing.B) {
	bench := newBenchRun(b)
	bench.topic = "bench100snappy"
	bench.pointPerMsg = 100
	bench.run(true)
}

// benchmark processing 1 metricPoint per kafka message.
// No compression is used with the kafka message.
func Benchmark1PointPerMessageRaw(b *testing.B) {
	bench := newBenchRun(b)
	bench.config.Producer.Compression = sarama.CompressionNone
	bench.topic = "bench1raw"
	bench.run(false)
}

// benchmark processing 10 metricPoints per kafka message.
// No compression is used with the kafka messages.
func Benchmark10PointPerMessageRaw(b *testing.B) {
	bench := newBenchRun(b)
	bench.config.Producer.Compression = sarama.CompressionNone
	bench.topic = "bench10raw"
	bench.pointPerMsg = 10
	bench.run(true)
}

// benchmark processing 50 metricPoints per kafka message.
// No compression is used with the kafka messages.
func Benchmark50PointPerMessageRaw(b *testing.B) {
	bench := newBenchRun(b)
	bench.config.Producer.Compression = sarama.CompressionNone
	bench.topic = "bench50raw"
	bench.pointPerMsg = 50
	bench.run(true)
}

// benchmark processing 100 metricPoints per kafka message.
// No compression is used with the kafka messages.
func Benchmark100PointPerMessageRaw(b *testing.B) {
	bench := newBenchRun(b)
	bench.config.Producer.Compression = sarama.CompressionNone
	bench.topic = "bench100raw"
	bench.pointPerMsg = 100
	bench.run(true)
}

func (b *benchRun) run(batched bool) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// push messages into kafka
	b.RunParallel(func(pb *testing.PB) {
		var keys []string
		var err error
		i := 0
		for pb.Next() {
			i++
			keys = append(keys, newKey(i))
			if len(keys) >= b.batchSize {
				if batched {
					err = b.sendBatched(keys)
				} else {
					err = b.send(keys)
				}
				if err != nil {
					log.Error(err)
					cancel()
					return
				}
				keys = keys[:0]
			}
		}
		if len(keys) > 0 {
			if batched {
				err = b.sendBatched(keys)
			} else {
				err = b.send(keys)
			}
			if err != nil {
				log.Error(err)
				cancel()
				return
			}
		}
	})

	client, err := sarama.NewClient([]string{"localhost:9092"}, b.config)
	if err != nil {
		b.Fatalf("failed to create new sarama client for consumer. %s", err)
	}
	defer client.Close()
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		b.Fatalf("failed to creat consumer. %s", err)
	}
	pc, err := consumer.ConsumePartition(b.topic, b.partition, sarama.OffsetOldest)
	if err != nil {
		b.Fatalf("failed to consume from partition. %s", err)
	}
	b.pc = pc
	b.ResetTimer()
	go func() {
		b.consume(ctx)
		cancel()
	}()
	<-ctx.Done()
}

func newKey(i int) string {
	return fmt.Sprintf("%d.%x", 123456, md5.Sum([]byte(strconv.Itoa(i))))
}

func (b *benchRun) send(keys []string) error {
	payload := make([]*sarama.ProducerMessage, len(keys))
	now := uint32(time.Now().Unix())
	var err error
	var data []byte
	for j := 0; j < len(keys); j++ {
		data = b.bufferPool33.Get()
		mkey, _ := schema.MKeyFromString(keys[j])
		mp := schema.MetricPoint{
			MKey:  mkey,
			Value: 0.1,
			Time:  now,
		}
		data = data[:1]
		data[0] = byte(msg.FormatMetricPoint) // store version in first byte
		data, err = mp.Marshal32(data)
		if err != nil {
			return err
		}
		payload[j] = &sarama.ProducerMessage{
			Partition: b.partition,
			Topic:     b.topic,
			Value:     sarama.ByteEncoder(data),
		}
	}
	defer func() {
		var buf []byte
		for _, msg := range payload {
			buf, _ = msg.Value.Encode()
			if cap(buf) == 33 {
				b.bufferPool33.Put(buf)
			} else {
				b.bufferPool.Put(buf)
			}
		}
	}()
	err = b.producer.SendMessages(payload)
	if err != nil {
		log.Errorf("failed to send message to kafka. %s", err)
		return err
	}

	return nil
}

func (b *benchRun) sendBatched(keys []string) error {
	var payload []*sarama.ProducerMessage
	now := uint32(time.Now().Unix())
	var err error
	i := 0

	for i < len(keys) {
		data := b.bufferPool.Get()
		msgCount := len(keys) - i
		if msgCount > b.pointPerMsg {
			msgCount = b.pointPerMsg
		}

		for j := 0; j < msgCount; j++ {
			mkey, _ := schema.MKeyFromString(keys[i+j])
			mp := schema.MetricPoint{
				MKey:  mkey,
				Value: 0.1,
				Time:  now,
			}
			data = append(data, byte(msg.FormatMetricPoint)) // store version in first byte
			data, err = mp.Marshal(data)
			if err != nil {
				return err
			}
		}
		i += msgCount
		payload = append(payload, &sarama.ProducerMessage{
			Partition: b.partition,
			Topic:     b.topic,
			Value:     sarama.ByteEncoder(data),
		})
	}
	defer func() {
		var buf []byte
		for _, msg := range payload {
			buf, _ = msg.Value.Encode()
			if cap(buf) == 33 {
				b.bufferPool33.Put(buf)
			} else {
				b.bufferPool.Put(buf)
			}
		}
	}()
	err = b.producer.SendMessages(payload)
	if err != nil {
		log.Errorf("failed to send message to kafka. %s", err)
		return err
	}

	return nil
}

func (b *benchRun) consume(ctx context.Context) error {
	done := ctx.Done()
	messages := b.pc.Messages()
LOOP:
	for {
		select {
		case m, ok := <-messages:
			// https://github.com/Shopify/sarama/wiki/Frequently-Asked-Questions#why-am-i-getting-a-nil-message-from-the-sarama-consumer
			if !ok {
				log.Errorf("kafka-mdm: kafka consumer for %s:%d has shutdown. stop consuming", b.topic, b.partition)
				return fmt.Errorf("consumer shutdown")
			}

			data := m.Value
			var point schema.MetricPoint
			var err error
			for len(data) > 0 {
				switch msg.Format(data[0]) {
				case msg.FormatMetricPointWithoutOrg:
					_, point, err = msg.ReadPointMsg(data[:29], 1)
					data = data[29:]
				case msg.FormatMetricPoint:
					_, point, err = msg.ReadPointMsg(data[:33], 1)
					data = data[33:]
				default:
					log.Error("got invalid data in message. %s", data)
					return fmt.Errorf("invalid data in message")
				}

				if err != nil {
					log.Error(3, "kafka-mdm decode error. %s", err)
					return err
				}
				if !point.Valid() {
					log.Error("received invalid metricpoint.")
					return fmt.Errorf("invalid metricPoint")
				}
				b.pointsRecvd++

			}

			if b.pointsRecvd >= b.N {
				log.Info("consumer received all messages")
				b.pc.Close()
				log.Infof("kafka-mdm consumer for %s:%d ended.", b.topic, b.partition)
				break LOOP
			}
		case <-done:
			log.Info("consumer received shutdown signal")
			b.pc.Close()
			log.Infof("kafka-mdm consumer for %s:%d ended.", b.topic, b.partition)
			break LOOP
		}
	}
	return nil
}
