package nsq

import (
	"time"

	n "github.com/nsqio/go-nsq"
	"github.com/raintank/met"
	"github.com/raintank/raintank-metric/msg"
	"github.com/raintank/raintank-metric/schema"
)

const NSQMaxMpubSize = 5 * 1024 * 1024 // nsq errors if more. not sure if can be changed
const NSQMaxMetricPerMsg = 1000        // empirically found through benchmarks (should result in 64~128k messages)

var (
	metricsPublished  met.Count
	messagesPublished met.Count
	messagesSize      met.Meter
	metricsPerMessage met.Meter
	publishDuration   met.Timer
)

type NSQ struct {
	topic    string
	producer *n.Producer
}

func New(topic, addr string, stats met.Backend) (*NSQ, error) {
	cfg := n.NewConfig()
	cfg.UserAgent = "fake_metrics"
	producer, err := n.NewProducer(addr, cfg)
	if err != nil {
		return nil, err
	}
	err = producer.Ping()
	if err != nil {
		return nil, err
	}

	metricsPublished = stats.NewCount("metricpublisher.nsq.metrics-published")
	messagesPublished = stats.NewCount("metricpublisher.nsq.messages-published")
	messagesSize = stats.NewMeter("metricpublisher.nsq.message_size", 0)
	metricsPerMessage = stats.NewMeter("metricpublisher.nsq.metrics_per_message", 0)
	publishDuration = stats.NewTimer("metricpublisher.nsq.publish_duration", 0)

	return &NSQ{
		topic:    topic,
		producer: producer,
	}, nil
}

func (n *NSQ) Close() error {
	n.producer.Stop()
	return nil
}

func (n *NSQ) Publish(metrics []*schema.MetricData) error {
	if len(metrics) == 0 {
		return nil
	}
	// typical metrics seem to be around 300B
	// nsqd allows <= 10MiB messages.
	// we ideally have 64kB ~ 1MiB messages (see benchmark https://gist.github.com/Dieterbe/604232d35494eae73f15)
	// at 300B, about 3500 msg fit in 1MiB
	// in worst case, this allows messages up to 2871B
	// this could be made more robust of course

	// real world findings in dev-stack with env-load:
	// 159569B msg /795  metrics per msg = 200B per msg
	// so peak message size is about 3500*200 = 700k (seen 711k)

	subslices := schema.Reslice(metrics, 3500)

	for _, subslice := range subslices {
		id := time.Now().UnixNano()
		data, err := msg.CreateMsg(subslice, id, msg.FormatMetricDataArrayMsgp)
		if err != nil {
			return err
		}

		messagesSize.Value(int64(len(data)))
		metricsPerMessage.Value(int64(len(subslice)))

		pre := time.Now()

		err = n.producer.Publish(n.topic, data)
		if err != nil {
			return err
		}

		publishDuration.Value(time.Since(pre))
		metricsPublished.Inc(int64(len(subslice)))
		messagesPublished.Inc(1)

		if err != nil {
			return err
		}
	}
	return nil
}
