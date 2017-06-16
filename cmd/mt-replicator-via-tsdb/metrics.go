package main

import (
	"bytes"
	"flag"
	"io/ioutil"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/bsm/sarama-cluster"
	"github.com/golang/snappy"
	"github.com/jpillora/backoff"
	m20 "github.com/metrics20/go-metrics20/carbon20"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
	"gopkg.in/raintank/schema.v1/msg"
)

var (
	producerBatchSize = flag.Int("batch-size", 10000, "number of metrics to send in each batch.")
	destinationUrl    = flag.String("destination-url", "http://localhost/metrics", "tsdb-gw address to send metrics to")
	destinationKey    = flag.String("destination-key", "admin-key", "admin-key of destination tsdb-gw server")

	group                = flag.String("group", "mt-replicator", "Kafka consumer group")
	orgId                = flag.Int("org-id", -1, "Organization ID of the metrics to be submitted")
	clientId             = flag.String("client-id", "mt-replicator", "Kafka consumer group client id")
	srcTopic             = flag.String("src-topic", "mdm", "metrics topic name on source cluster")
	initialOffset        = flag.Int("initial-offset", -2, "initial offset to consume from. (-2=oldest, -1=newest)")
	srcBrokerStr         = flag.String("src-brokers", "localhost:9092", "tcp address of source kafka cluster (may be be given multiple times as a comma-separated list)")
	consumerFetchDefault = flag.Int("consumer-fetch-default", 32768, "number of bytes to try and fetch from consumer")
)

type writeRequest struct {
	data      []byte
	count     int
	offset    int64
	topic     string
	partition int32
}

type MetricsReplicator struct {
	consumer    *cluster.Consumer
	tsdbClient  *http.Client
	tsdbUrl     string
	tsdbKey     string
	writeQueue  chan *writeRequest
	wg          sync.WaitGroup
	shutdown    chan struct{}
	flushBuffer chan []*schema.MetricData
	badCount    int
}

func NewMetricsReplicator() (*MetricsReplicator, error) {
	if *group == "" {
		log.Fatal(4, "--group is required")
	}

	if *srcBrokerStr == "" {
		log.Fatal(4, "--src-brokers required")
	}
	if *srcTopic == "" {
		log.Fatal(4, "--src-topic is required")
	}

	srcBrokers := strings.Split(*srcBrokerStr, ",")

	config := cluster.NewConfig()
	config.Consumer.Offsets.Initial = int64(*initialOffset)
	config.ClientID = *clientId
	config.Group.Return.Notifications = true
	config.ChannelBufferSize = 1000
	config.Consumer.Fetch.Min = 1
	config.Consumer.Fetch.Default = int32(*consumerFetchDefault)
	config.Consumer.MaxWaitTime = time.Second
	config.Consumer.MaxProcessingTime = time.Second * time.Duration(5)
	config.Config.Version = sarama.V0_10_0_0

	err := config.Validate()
	if err != nil {
		return nil, err
	}
	consumer, err := cluster.NewConsumer(srcBrokers, *group, []string{*srcTopic}, config)
	if err != nil {
		log.Error(3, "failed to connect to source brokers %v.", srcBrokers)
		return nil, err
	}
	tsdbClient := &http.Client{
		Timeout: time.Duration(10) * time.Second,
	}

	return &MetricsReplicator{
		consumer:   consumer,
		tsdbClient: tsdbClient,
		tsdbKey:    *destinationKey,
		tsdbUrl:    *destinationUrl,

		shutdown:   make(chan struct{}),
		writeQueue: make(chan *writeRequest, 10),
	}, nil
}

func (r *MetricsReplicator) Consume() {
	buf := make([]*schema.MetricData, 0)
	accountingTicker := time.NewTicker(time.Second * 10)
	flushTicker := time.NewTicker(time.Second)
	counter := 0
	counterTs := time.Now()
	msgChan := r.consumer.Messages()

	flush := func(topic string, partition int32, offset int64) {
		data, err := msg.CreateMsg(buf, 0, msg.FormatMetricDataArrayMsgp)
		if err != nil {
			panic(err)
		}
		// this will block when the writeQueue fills up. This will happen if
		// we are consuming at a faster rate then we can publish, or if publishing
		// is failing for some reason.
		r.writeQueue <- &writeRequest{
			data:      data,
			topic:     topic,
			partition: partition,
			offset:    offset,
			count:     len(buf),
		}
		counter += len(buf)
		buf = buf[:0]
	}

	var m *sarama.ConsumerMessage
	var ok bool
	defer func() {
		close(r.writeQueue)
		r.wg.Done()
	}()
	for {
		select {
		case m, ok = <-msgChan:
			if !ok {
				if len(buf) != 0 {
					flush(m.Topic, m.Partition, m.Offset)
				}
				return
			}
			for _, line := range bytes.Split(m.Value, []byte("\n")) {
				if len(line) == 0 {
					continue
				}
				key, val, ts, err := m20.ValidatePacket(
					line,
					m20.NoneLegacy,
					m20.NoneM20,
				)
				if err != nil {
					log.Error("Failed to parse msg")
					r.badCount++
					continue
				}
				keyStr := string(key)
				md := &schema.MetricData{
					Name:     keyStr,
					Metric:   keyStr,
					Time:     int64(ts),
					Mtype:    "gauge",
					Interval: 1,
					Value:    val,
					OrgId:    *orgId,
				}
				buf = append(buf, md)
			}

			if len(buf) > *producerBatchSize {
				flush(m.Topic, m.Partition, m.Offset)
				// reset our ticker
				flushTicker.Stop()
				flushTicker = time.NewTicker(time.Second)
			}
		case <-flushTicker.C:
			if len(buf) == 0 {
				continue
			}
			flush(m.Topic, m.Partition, m.Offset)
		case t := <-accountingTicker.C:
			log.Info("%d metrics processed in last %.1fseconds.", counter, t.Sub(counterTs).Seconds())
			counter = 0
			counterTs = t
		case <-r.shutdown:
			if r.badCount > 0 {
				log.Error("Failed to parse %d msgs.", r.badCount)
			}
			if len(buf) > 0 {
				flush(m.Topic, m.Partition, m.Offset)
			}
			return
		}
	}
}

func (r *MetricsReplicator) Stop() {
	r.consumer.Close()
	close(r.shutdown)
	log.Info("Consumer closed.")
	done := make(chan struct{})
	go func() {
		r.wg.Wait()
		close(done)
	}()

	select {
	case <-time.After(time.Minute):
		log.Info("shutdown not complete after 1 minute. Abandoning inflight data.")
	case <-done:
		log.Info("shutdown complete.")
	}
	return
}

func (r *MetricsReplicator) Start() {
	r.wg.Add(1)
	go r.Consume()
	r.wg.Add(1)
	go r.Flush()
}

func (r *MetricsReplicator) Flush() {
	b := &backoff.Backoff{
		Min:    100 * time.Millisecond,
		Max:    time.Minute,
		Factor: 1.5,
		Jitter: true,
	}
	body := new(bytes.Buffer)
	defer r.wg.Done()
	for wr := range r.writeQueue {
		for {
			pre := time.Now()
			body.Reset()
			snappyBody := snappy.NewWriter(body)
			snappyBody.Write(wr.data)
			req, err := http.NewRequest("POST", r.tsdbUrl, body)
			if err != nil {
				panic(err)
			}
			req.Header.Add("Authorization", "Bearer "+r.tsdbKey)
			req.Header.Add("Content-Type", "rt-metric-binary-snappy")
			resp, err := r.tsdbClient.Do(req)
			diff := time.Since(pre)
			if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
				// the payload has been successfully sent so lets mark our offset
				r.consumer.MarkPartitionOffset(wr.topic, wr.partition, wr.offset, "")

				b.Reset()
				log.Info("GrafanaNet sent %d metrics in %s -msg size %d", wr.count, diff, body.Len())
				resp.Body.Close()
				ioutil.ReadAll(resp.Body)
				break
			}
			dur := b.Duration()
			if err != nil {
				log.Warn("GrafanaNet failed to submit data: %s will try again in %s (this attempt took %s)", err, dur, diff)
			} else {
				buf := make([]byte, 300)
				n, _ := resp.Body.Read(buf)
				log.Warn("GrafanaNet failed to submit data: http %d - %s will try again in %s (this attempt took %s)", resp.StatusCode, buf[:n], dur, diff)
				resp.Body.Close()
				ioutil.ReadAll(resp.Body)
			}

			time.Sleep(dur)
		}
	}
}
