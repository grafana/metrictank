package out

import "github.com/raintank/raintank-metric/schema"

type Out interface {
	Close() error
	Publish(metrics []*schema.MetricData) error
}
