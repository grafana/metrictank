package main

import (
	"bytes"
	"io/ioutil"
	"net/http"
	"sync"
	"time"

	"github.com/golang/snappy"
	"github.com/jpillora/backoff"
	"github.com/raintank/metrictank/cluster"
	"github.com/raintank/metrictank/stats"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
	"gopkg.in/raintank/schema.v1/msg"
)

var (
	publishDuration  = stats.NewLatencyHistogram15s32("metrics.publish")
	metricsPublished = stats.NewCounter32("metrics.published")
	messagesSize     = stats.NewMeter32("metrics.message_size", false)
	sendErrProducer  = stats.NewCounter32("metrics.send_error.producer")
)

type writeRequest struct {
	data  []byte
	count int
}

type MetricsReplicator struct {
	tsdbClient    *http.Client
	tsdbUrl       string
	tsdbKey       string
	writeQueue    map[int32]chan *writeRequest
	wg            sync.WaitGroup
	partitionChan map[int32]chan *schema.MetricData
}

func NewMetricsReplicator(destinationURL, destinationKey string) *MetricsReplicator {
	r := &MetricsReplicator{
		tsdbClient: &http.Client{
			Timeout: time.Duration(10) * time.Second,
		},
		tsdbKey: destinationKey,
		tsdbUrl: destinationURL,
	}

	return r
}

func (r *MetricsReplicator) Start() {
	partitionChan := make(map[int32]chan *schema.MetricData)
	writeQueue := make(map[int32]chan *writeRequest)
	for _, part := range cluster.Manager.GetPartitions() {
		partitionChan[part] = make(chan *schema.MetricData)
		writeQueue[part] = make(chan *writeRequest, 10)
	}

	r.partitionChan = partitionChan
	r.writeQueue = writeQueue

	// as our goroutines read r.partitionChan and r.writeQueue we
	// start them after these maps have been populated completely.
	// Otherwise we would need to add synchronization to prevent
	// concurrent read/writes.
	for _, part := range cluster.Manager.GetPartitions() {
		r.wg.Add(1)
		go r.consume(part)
		r.wg.Add(1)
		go r.flush(part)
	}

}

func (r *MetricsReplicator) Stop() {
	for _, ch := range r.partitionChan {
		// close consumer channels.  This will cause the consume() goroutines to end
		// which will inturn cause the writeQueue chan to be closed leading to the
		// flush() goroutines to end.
		close(ch)
	}
	log.Info("Consumer closed.")
	done := make(chan struct{})
	go func() {
		// wait for all of our goroutines (consume() and flush()) to end.
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

func (r *MetricsReplicator) Process(metric *schema.MetricData, partition int32) {
	// write the metric to the channel for this partition.  This map of chans is created
	// at startup and never modified again, so it is safe to read without synchonization.
	r.partitionChan[partition] <- metric
}

func (r *MetricsReplicator) consume(partition int32) {
	ch := r.partitionChan[partition]
	flushTicker := time.NewTicker(time.Second)
	buf := make([]*schema.MetricData, 0)

	flush := func() {
		mda := schema.MetricDataArray(buf)
		data, err := msg.CreateMsg(mda, 0, msg.FormatMetricDataArrayMsgp)
		if err != nil {
			log.Fatal(4, err.Error())
		}
		// this will block when the writeQueue fills up. This will happen if
		// we are consuming at a faster rate then we can publish, or if publishing
		// is failing for some reason.
		r.writeQueue[partition] <- &writeRequest{
			data:  data,
			count: len(buf),
		}
		buf = buf[:0]
	}

	var md *schema.MetricData
	var ok bool
	defer func() {
		close(r.writeQueue[partition])
		r.wg.Done()
		log.Info("consumer for partition %d has stopped", partition)
	}()
	log.Info("consumer for partition %d has started", partition)
	for {
		select {
		case md, ok = <-ch:
			if !ok {
				// input chan has been closed.
				if len(buf) != 0 {
					flush()
				}
				return
			}
			buf = append(buf, md)
			if len(buf) > *producerBatchSize {
				flush()
				// reset our ticker
				flushTicker.Stop()
				flushTicker = time.NewTicker(time.Second)
			}
		case <-flushTicker.C:
			if len(buf) == 0 {
				continue
			}
			flush()
		}
	}
}

func (r *MetricsReplicator) flush(partition int32) {
	q := r.writeQueue[partition]
	b := &backoff.Backoff{
		Min:    100 * time.Millisecond,
		Max:    time.Minute,
		Factor: 1.5,
		Jitter: true,
	}
	body := new(bytes.Buffer)
	defer r.wg.Done()
	for wr := range q {
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
				b.Reset()
				log.Debug("GrafanaNet sent %d metrics in %s -msg size %d", wr.count, diff, body.Len())
				resp.Body.Close()
				ioutil.ReadAll(resp.Body)
				publishDuration.Value(time.Since(pre))
				metricsPublished.Add(wr.count)
				messagesSize.Value(int(req.ContentLength))
				break
			}
			sendErrProducer.Inc()
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
