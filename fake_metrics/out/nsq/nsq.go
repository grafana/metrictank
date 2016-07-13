package nsq

import (
	"time"

	n "github.com/nsqio/go-nsq"
	"github.com/raintank/met"
	"github.com/raintank/raintank-metric/fake_metrics/out"
	"github.com/raintank/schema"
	"github.com/raintank/schema/msg"
)

const NSQMaxMpubSize = 5 * 1024 * 1024 // nsq errors if more. not sure if can be changed
const NSQMaxMetricPerMsg = 1000        // empirically found through benchmarks (should result in 64~128k messages)

type NSQ struct {
	out.OutStats
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

	return &NSQ{
		OutStats: out.NewStats(stats, "nsq"),
		topic:    topic,
		producer: producer,
	}, nil
}

func (n *NSQ) Close() error {
	n.producer.Stop()
	return nil
}

func (n *NSQ) Flush(metrics []*schema.MetricData) error {
	if len(metrics) == 0 {
		n.FlushDuration.Value(0)
		return nil
	}
	preFlush := time.Now()
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

		n.MessageBytes.Value(int64(len(data)))
		n.MessageMetrics.Value(int64(len(subslice)))

		prePub := time.Now()

		err = n.producer.Publish(n.topic, data)
		if err != nil {
			n.PublishErrors.Inc(1)
			return err
		}

		n.PublishedMetrics.Inc(int64(len(subslice)))
		n.PublishedMessages.Inc(1)
		n.PublishDuration.Value(time.Since(prePub))
	}
	n.FlushDuration.Value(time.Since(preFlush))
	return nil
}
