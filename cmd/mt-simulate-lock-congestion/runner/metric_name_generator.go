package runner

import (
	"fmt"
	"math"
	"strings"
	"sync/atomic"
)

// metricNameGenerator generates new metric names and has the ability to return at least
// one metric name which is guaranteed to have been returned
type metricNameGenerator interface {
	// writes a new metric name into the builder which has not been used yet
	getNewMetricName() string

	// writes a metric name into the builder which has already been returned by getNewMetricName()
	getExistingMetricName() string
}

type increasingNumberGenerator struct {
	lastUsedID uint64
}

func newMetricNameGenerator() metricNameGenerator {
	return &increasingNumberGenerator{}
}

func (s *increasingNumberGenerator) getNewMetricName() string {
	id := atomic.AddUint64(&s.lastUsedID, 1)

	if id == math.MaxUint64 {
		panic("Exhausted the whole range of uint64")
	}

	return s.buildMetricNameFromID(id)
}

func (s *increasingNumberGenerator) getExistingMetricName() string {
	return s.buildMetricNameFromID(atomic.LoadUint64(&s.lastUsedID))
}

func (s *increasingNumberGenerator) buildMetricNameFromID(id uint64) string {
	builder := strings.Builder{}
	for i, char := range fmt.Sprintf("%d", id) {
		if i > 0 {
			builder.WriteRune('.')
		}
		builder.WriteRune(char)
	}
	return builder.String()
}
