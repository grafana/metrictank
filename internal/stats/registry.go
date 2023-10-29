package stats

import (
	"fmt"
	"reflect"
	"sync"
)

var errFmtMetricExists = "fatal: metric %q already exists as type %T"

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

func (r *Registry) getOrAdd(name string, metric GraphiteMetric) GraphiteMetric {
	r.Lock()
	if existing, ok := r.metrics[name]; ok {
		if reflect.TypeOf(existing) == reflect.TypeOf(metric) {
			r.Unlock()
			return existing
		}
		panic(fmt.Sprintf(errFmtMetricExists, name, existing))
	}
	r.metrics[name] = metric
	r.Unlock()
	return metric
}

func (r *Registry) list() map[string]GraphiteMetric {
	metrics := make(map[string]GraphiteMetric)
	r.Lock()
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
