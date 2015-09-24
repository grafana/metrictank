package main

import "sync"

type AggMetrics struct {
	sync.Mutex
	metrics map[string]*AggMetric
}

func NewAggMetrics() *AggMetrics {
	ms := AggMetrics{
		metrics: make(map[string]*AggMetric),
	}
	return &ms
}

func (ms *AggMetrics) Get(key string) Metric {
	ms.Lock()
	m, ok := ms.metrics[key]
	if !ok {
		m = NewAggMetric(key)
		ms.metrics[key] = m
	}
	ms.Unlock()
	return m
}
