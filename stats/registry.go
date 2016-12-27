package stats

import (
	"fmt"
	"sync"
)

var errFmtMetricExists = "fatal: metric %q already exists"

// Registry tracks metrics and reporters
type Registry struct {
	sync.Mutex
	// here we use just the metric name as key. it does not include any prefix
	// this means that technically we're more strict here then needed around naming conflicts:
	// it's possible to have metrics with the same name, but a different prefix.
	// we still complain about that
	metrics map[string]GraphiteMetric
}

func NewRegistry() *Registry {
	return &Registry{
		metrics: make(map[string]GraphiteMetric),
	}
}

func (r *Registry) add(name string, getMetric func() GraphiteMetric) GraphiteMetric {
	r.Lock()
	if _, ok := r.metrics[name]; ok {
		panic(fmt.Sprintf(errFmtMetricExists, name))
	}
	m := getMetric()
	r.metrics[name] = m
	r.Unlock()
	return m
}

func (r *Registry) list() map[string]GraphiteMetric {
	r.Lock()
	metrics := make(map[string]GraphiteMetric)
	for name, metric := range r.metrics {
		metrics[name] = metric
	}
	r.Unlock()
	return metrics
}

func (r *Registry) Clear() {
	r.Lock()
	r.metrics = make(map[string]GraphiteMetric)
	r.Unlock()
}
