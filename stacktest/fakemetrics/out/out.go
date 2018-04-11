package out

import (
	"fmt"

	"github.com/raintank/met"
	"gopkg.in/raintank/schema.v1"
)

type Out interface {
	Close() error
	Flush(metrics []*schema.MetricData) error
}

type OutStats struct {
	FlushDuration     met.Timer // duration of Flush()
	PublishQueued     met.Gauge // not every output uses this
	PublishErrors     met.Count // not every output uses this
	PublishDuration   met.Timer // duration of writing a message to underlying storage
	PublishedMetrics  met.Count // number of metrics written to underlying storage
	PublishedMessages met.Count // number of messages written to underlying storage
	MessageBytes      met.Meter // number of bytes per message
	MessageMetrics    met.Meter // number of metrics per message
}

func NewStats(stats met.Backend, output string) OutStats {
	return OutStats{
		FlushDuration: stats.NewTimer(fmt.Sprintf("metricpublisher.out.%s.flush_duration", output), 0),

		PublishQueued: stats.NewGauge(fmt.Sprintf("metricpublisher.out.%s.publish_queued", output), 0),
		PublishErrors: stats.NewCount(fmt.Sprintf("metricpublisher.out.%s.publish_errors", output)),

		PublishDuration:   stats.NewTimer(fmt.Sprintf("metricpublisher.out.%s.publish_duration", output), 0),
		PublishedMetrics:  stats.NewCount(fmt.Sprintf("metricpublisher.out.%s.published_metrics", output)),
		PublishedMessages: stats.NewCount(fmt.Sprintf("metricpublisher.out.%s.published_messages", output)),

		MessageBytes:   stats.NewMeter(fmt.Sprintf("metricpublisher.out.%s.message_bytes", output), 0),
		MessageMetrics: stats.NewMeter(fmt.Sprintf("metricpublisher.out.%s.message_metrics", output), 0),
	}
}
