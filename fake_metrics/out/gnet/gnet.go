package gnet

import (
	"bytes"
	"crypto/tls"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/grafana/grafana/pkg/log"
	"github.com/raintank/met"
	"github.com/raintank/raintank-metric/msg"
	"github.com/raintank/raintank-metric/schema"
)

const safeSSL = false

var (
	metricsPublished  met.Count
	messagesPublished met.Count
	messagesSize      met.Meter
	metricsPerMessage met.Meter
	publishDuration   met.Timer
)

type Gnet struct {
	url    string
	key    string
	bearer string
	client *http.Client
}

func New(url, key string, stats met.Backend) (*Gnet, error) {
	client := &http.Client{}

	if !safeSSL {
		// this transport should be the equivalent of Go's DefaultTransport
		client.Transport = &http.Transport{
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

	metricsPublished = stats.NewCount("metricpublisher.gnet.metrics-published")
	messagesPublished = stats.NewCount("metricpublisher.gnet.messages-published")
	messagesSize = stats.NewMeter("metricpublisher.gnet.message_size", 0)
	metricsPerMessage = stats.NewMeter("metricpublisher.gnet.metrics_per_message", 0)
	publishDuration = stats.NewTimer("metricpublisher.gnet.publish_duration", 0)

	return &Gnet{
		url:    url,
		key:    key,
		bearer: fmt.Sprintf("Bearer " + key),
		client: client,
	}, nil
}

func (g *Gnet) Close() error {
	return nil
}

func (g *Gnet) Publish(metrics []*schema.MetricData) error {
	if len(metrics) == 0 {
		return nil
	}
	subslices := schema.Reslice(metrics, 3500)

	for _, subslice := range subslices {
		log.Debug("gnet asked to publish %d metrics at ts %s", len(subslice), time.Unix(subslice[0].Time, 0))
		metrics := schema.MetricDataArray(subslice)
		data, err := msg.CreateMsg(metrics, 0, msg.FormatMetricDataArrayMsgp)
		if err != nil {
			return err
		}

		messagesSize.Value(int64(len(data)))
		metricsPerMessage.Value(int64(len(subslice)))

		pre := time.Now()

		req, err := http.NewRequest("POST", g.url, bytes.NewBuffer(data))
		if err != nil {
			return err
		}
		req.Header.Add("Authorization", g.bearer)
		req.Header.Add("Content-Type", "rt-metric-binary")
		resp, err := g.client.Do(req)
		if err != nil {
			return err
		}
		if resp.StatusCode != 200 {
			return fmt.Errorf("http response was %d - %s", resp.StatusCode, resp.Status)
		}

		publishDuration.Value(time.Since(pre))
		metricsPublished.Inc(int64(len(subslice)))
		messagesPublished.Inc(1)
	}
	return nil
}
