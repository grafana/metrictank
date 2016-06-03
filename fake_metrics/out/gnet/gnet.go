package gnet

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/grafana/grafana/pkg/log"
	"github.com/jpillora/backoff"
	"github.com/raintank/met"
	"github.com/raintank/raintank-metric/msg"
	"github.com/raintank/raintank-metric/schema"
)

var (
	// standard metrics that many outputs have
	metricsPublished  met.Count
	messagesPublished met.Count
	messagesSize      met.Meter
	metricsPerMessage met.Meter
	publishDuration   met.Timer

	// extra metrics
	numErrFlush met.Count
	numQueued   met.Gauge
)

type Gnet struct {
	url    string
	key    string
	bearer string
	client *http.Client

	bufSize      int // amount of messages we can buffer up before providing backpressure.
	flushMaxNum  int
	flushMaxWait time.Duration
	timeout      time.Duration
	sslVerify    bool

	queue chan []*schema.MetricData
}

func New(url, key string, stats met.Backend) (*Gnet, error) {

	bufSize := 100
	flushMaxNum := 2000
	flushMaxWait := 200 * time.Millisecond
	timeout := 3 * time.Second
	sslVerify := false

	metricsPublished = stats.NewCount("metricpublisher.gnet.metrics-published")
	messagesPublished = stats.NewCount("metricpublisher.gnet.messages-published")
	messagesSize = stats.NewMeter("metricpublisher.gnet.message_size", 0)
	metricsPerMessage = stats.NewMeter("metricpublisher.gnet.metrics_per_message", 0)
	publishDuration = stats.NewTimer("metricpublisher.gnet.publish_duration", 0)

	numErrFlush = stats.NewCount("metricpublisher.gnet.flush_err")
	numQueued = stats.NewGauge("metricpublisher.gnet.queued", 0)

	gnet := &Gnet{
		url:    url,
		key:    key,
		bearer: fmt.Sprintf("Bearer " + key),
		client: &http.Client{
			Timeout: timeout,
		},

		bufSize:      bufSize,
		flushMaxNum:  flushMaxNum,
		flushMaxWait: flushMaxWait,
		timeout:      timeout,
		sslVerify:    sslVerify,

		queue: make(chan []*schema.MetricData, bufSize),
	}

	if !sslVerify {
		// this transport should be the equivalent of Go's DefaultTransport
		gnet.client.Transport = &http.Transport{
			Proxy: http.ProxyFromEnvironment,
			Dial: (&net.Dialer{
				Timeout:   30 * time.Second,
				KeepAlive: 30 * time.Second,
			}).Dial,
			TLSHandshakeTimeout:   10 * time.Second,
			ExpectContinueTimeout: 1 * time.Second,
			// except for this
			TLSClientConfig: &tls.Config{
				InsecureSkipVerify: true,
			},
		}
	}

	go gnet.run()
	return gnet, nil
}

func (g *Gnet) Close() error {
	return nil
}

func (g *Gnet) Publish(metrics []*schema.MetricData) error {
	if len(metrics) == 0 {
		return nil
	}
	log.Debug("gnet asked to publish %d metrics at ts %s", len(metrics), time.Unix(metrics[0].Time, 0))
	numQueued.Inc(int64(len(metrics)))
	data := make([]*schema.MetricData, len(metrics))
	copy(data, metrics)
	g.queue <- data
	return nil
}

func (g *Gnet) run() {
	metrics := make([]*schema.MetricData, 0, g.flushMaxNum)
	ticker := time.NewTicker(g.flushMaxWait)

	for {
		select {
		case buf := <-g.queue:
			// incoming buf needs to be flushed. but the data it brings may not fill up the queue,
			// it may fill it up exactly, or it may be more than the available slots in the queue.
			// more so, it may be so large we need to fill the queue multiple times to get through it.
			numQueued.Dec(int64(len(buf)))
			slots := cap(metrics) - len(metrics)
			todoI := 0
			for len(buf)-todoI >= slots {
				todoJ := todoI + slots
				metrics = append(metrics, buf[todoI:todoJ]...)
				g.flush(metrics)
				metrics, slots = metrics[:0], cap(metrics)
				todoI = todoJ
			}
			// at this point todo < slots.
			// todo may be 0, that's ok. metrics may have len 0 or not, that's ok.
			// we know we don't have to flush as there is still room.
			metrics = append(metrics, buf[todoI:]...)
		case <-ticker.C:
			g.flush(metrics)
			metrics = metrics[:0]
		}
	}
}

func (g *Gnet) flush(metrics []*schema.MetricData) {
	if len(metrics) == 0 {
		return
	}

	b := &backoff.Backoff{
		Min:    100 * time.Millisecond,
		Max:    time.Minute,
		Factor: 1.5,
		Jitter: true,
	}

	mda := schema.MetricDataArray(metrics)
	data, err := msg.CreateMsg(mda, 0, msg.FormatMetricDataArrayMsgp)
	if err != nil {
		panic(err)
	}
	for {
		messagesSize.Value(int64(len(data)))
		metricsPerMessage.Value(int64(len(mda)))
		pre := time.Now()
		req, err := http.NewRequest("POST", g.url, bytes.NewBuffer(data))
		if err != nil {
			panic(err)
		}
		req.Header.Add("Authorization", g.bearer)
		req.Header.Add("Content-Type", "rt-metric-binary")
		resp, err := g.client.Do(req)
		diff := time.Since(pre)

		if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
			log.Info("GrafanaNet sent %d metrics in %s -msg size %d", len(metrics), diff, len(data))
			b.Reset()
			resp.Body.Close()
			publishDuration.Value(diff)
			metricsPublished.Inc(int64(len(metrics)))
			messagesPublished.Inc(1)
			break
		}

		numErrFlush.Inc(1)
		dur := b.Duration()
		if err != nil {
			log.Warn("GrafanaNet failed to submit data: %s will try again in %s (this attempt took %s)", err, dur, diff)
		} else {
			buf := make([]byte, 300)
			n, _ := resp.Body.Read(buf)
			log.Warn("GrafanaNet failed to submit data: http %d - %s: %s will try again in %s (this attempt took %s)", resp.StatusCode, resp.Status, buf[:n], dur, diff)
			resp.Body.Close()
		}

		time.Sleep(dur)
	}
}
