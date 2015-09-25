package main

import "sync"
import gometrics "github.com/rcrowley/go-metrics"
import "time"

var points = gometrics.NewHistogram(gometrics.NewExpDecaySample(1028, 0.015))

type AggMetrics struct {
	sync.Mutex
	metrics   map[string]*AggMetric
	chunkSpan uint32
	numChunks uint32
}

func NewAggMetrics(chunkSpan, numChunks uint32) *AggMetrics {
	ms := AggMetrics{
		metrics:   make(map[string]*AggMetric),
		chunkSpan: chunkSpan,
		numChunks: numChunks,
	}
	go ms.stats()
	return &ms
}

func (ms *AggMetrics) stats() {
	gometrics.Register("points_per_metric", points)
	points.Update(0)

	metricsActive := gometrics.NewGauge()
	gometrics.Register("metrics_active", metricsActive)
	for range time.Tick(time.Duration(1) * time.Second) {
		metricsActive.Update(int64(len(ms.metrics)))
	}
}

func (ms *AggMetrics) Get(key string) Metric {
	ms.Lock()
	m, ok := ms.metrics[key]
	if !ok {
		m = NewAggMetric(key, ms.chunkSpan, ms.numChunks)
		ms.metrics[key] = m
	}
	ms.Unlock()
	return m
}
