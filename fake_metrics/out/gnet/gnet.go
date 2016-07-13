package gnet

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/jpillora/backoff"
	"github.com/raintank/met"
	"github.com/raintank/raintank-metric/fake_metrics/out"
	"github.com/raintank/schema"
	"github.com/raintank/schema/msg"
	"github.com/raintank/worldping-api/pkg/log"
)

type Msg struct {
	data []byte
	num  int // metrics contained within
}

type Gnet struct {
	out.OutStats

	url    string
	key    string
	bearer string
	client *http.Client

	bufSize   int // amount of messages we can buffer up before providing backpressure.
	timeout   time.Duration
	sslVerify bool

	queue chan Msg
}

func New(url, key string, stats met.Backend) (*Gnet, error) {

	bufSize := 100
	timeout := 3 * time.Second
	sslVerify := false

	gnet := &Gnet{
		OutStats: out.NewStats(stats, "gnet"),

		url:    url,
		key:    key,
		bearer: fmt.Sprintf("Bearer " + key),
		client: &http.Client{
			Timeout: timeout,
		},

		bufSize:   bufSize,
		timeout:   timeout,
		sslVerify: sslVerify,

		queue: make(chan Msg, bufSize),
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

func (g *Gnet) Flush(metrics []*schema.MetricData) error {
	if len(metrics) == 0 {
		g.FlushDuration.Value(0)
		return nil
	}
	preFlush := time.Now()
	log.Debug("gnet asked to publish %d metrics at ts %s", len(metrics), time.Unix(metrics[0].Time, 0))
	mda := schema.MetricDataArray(metrics)
	data, err := msg.CreateMsg(mda, 0, msg.FormatMetricDataArrayMsgp)
	if err != nil {
		panic(err)
	}
	g.PublishQueued.Inc(int64(len(metrics)))
	g.queue <- Msg{data, len(metrics)}
	g.FlushDuration.Value(time.Since(preFlush))
	return nil
}

func (g *Gnet) run() {
	for m := range g.queue {
		g.PublishQueued.Dec(int64(m.num))
		prePub := time.Now()
		g.publish(m)
		g.PublishDuration.Value(time.Since(prePub))
	}
}

func (g *Gnet) publish(m Msg) {
	b := &backoff.Backoff{
		Min:    100 * time.Millisecond,
		Max:    time.Minute,
		Factor: 1.5,
		Jitter: true,
	}

	for {
		g.MessageBytes.Value(int64(len(m.data)))
		g.MessageMetrics.Value(int64(m.num))
		pre := time.Now()
		req, err := http.NewRequest("POST", g.url, bytes.NewBuffer(m.data))
		if err != nil {
			panic(err)
		}
		req.Header.Add("Authorization", g.bearer)
		req.Header.Add("Content-Type", "rt-metric-binary")
		resp, err := g.client.Do(req)
		diff := time.Since(pre)

		if err == nil && resp.StatusCode >= 200 && resp.StatusCode < 300 {
			log.Info("GrafanaNet sent %d metrics in %s -msg size %d", m.num, diff, len(m.data))
			b.Reset()
			resp.Body.Close()
			g.PublishedMetrics.Inc(int64(m.num))
			g.PublishedMessages.Inc(1)
			break
		}

		g.PublishErrors.Inc(1)
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
