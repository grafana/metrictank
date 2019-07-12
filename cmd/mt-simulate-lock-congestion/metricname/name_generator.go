package metricname

import (
	"context"
)

// NameGenerator generates new metric names and has the ability to return at least
// one metric name which is guaranteed to have been returned
type NameGenerator interface {
	// writes a new metric name into the builder which has not been used yet
	GetNewMetricName() string

	// starts the metric name generator
	Start(ctx context.Context, threadCount uint32)
}
