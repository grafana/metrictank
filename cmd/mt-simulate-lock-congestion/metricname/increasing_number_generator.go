package metricname

import (
	"context"
	"fmt"
	"log"
	"math"
	"strings"
	"sync/atomic"
)

type increasingNumberGenerator struct {
	lastUsedID    uint64
	lastUsedValue atomic.Value
	nameChan      chan string
}

func NewIncreasingNumberGenerator() NameGenerator {
	return &increasingNumberGenerator{}
}

func (s *increasingNumberGenerator) GetNewMetricName() string {
	name := <-s.nameChan
	s.lastUsedValue.Store(name)
	return name
}

func (s *increasingNumberGenerator) GetExistingMetricName() string {
	return <-s.nameChan
}

func (s *increasingNumberGenerator) Start(ctx context.Context, threadCount uint32) {
	s.nameChan = make(chan string, 1000)
	for i := uint32(0); i < threadCount; i++ {
		go s.generateNames(ctx)
	}
}

func (s *increasingNumberGenerator) generateNames(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
		default:
			id := atomic.AddUint64(&s.lastUsedID, 1)
			if id == math.MaxUint64 {
				log.Fatal("Exhausted the whole range of uint64")
			}
			s.nameChan <- s.buildMetricNameFromID(id)
		}
	}
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
