package memorystore

import (
	"sync"
	"time"

	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/stats"
	"github.com/raintank/worldping-api/pkg/log"
	"gopkg.in/raintank/schema.v1"
)

var (
	// metric tank.metrics_active is the number of currently known metrics (excl rollup series), measured every second
	metricsActive = stats.NewGauge32("tank.metrics_active")

	// metric tank.gc_metric is the number of times the metrics GC is about to inspect a metric (series)
	gcMetric = stats.NewCounter32("tank.gc_metric")
)

type AggMetrics struct {
	Metrics        sync.Map
	chunkMaxStale  uint32
	metricMaxStale uint32
	gcInterval     time.Duration
}

func NewAggMetrics(chunkMaxStale, metricMaxStale uint32, gcInterval time.Duration) *AggMetrics {
	ms := AggMetrics{
		chunkMaxStale:  chunkMaxStale,
		metricMaxStale: metricMaxStale,
		gcInterval:     gcInterval,
	}

	// gcInterval = 0 can be useful in tests
	if gcInterval > 0 {
		go ms.GC()
	}
	return &ms
}

// periodically scan chunks and close any that have not received data in a while
func (ms *AggMetrics) GC() {
	for {
		unix := time.Duration(time.Now().UnixNano())
		diff := ms.gcInterval - (unix % ms.gcInterval)
		time.Sleep(diff + time.Minute)
		log.Info("checking for stale chunks that need persisting.")
		now := uint32(time.Now().Unix())
		chunkMinTs := now - uint32(ms.chunkMaxStale)
		metricMinTs := now - uint32(ms.metricMaxStale)

		ms.Metrics.Range(func(k, a interface{}) bool {
			gcMetric.Inc()
			if stale := a.(mdata.Metric).GC(chunkMinTs, metricMinTs); stale {
				log.Debug("metric %s is stale. Purging data from memory.", k)
				ms.Metrics.Delete(k)
				metricsActive.Dec()
			}
			return true
		})
	}
}

func (ms *AggMetrics) Get(key string) (mdata.Metric, bool) {
	m, ok := ms.Metrics.Load(key)
	return m.(mdata.Metric), ok
}

func (ms *AggMetrics) Store(key string, metric mdata.Metric) {
	ms.Metrics.Store(key, metric)
}

func (ms *AggMetrics) LoadOrStore(key string, metric mdata.Metric) (mdata.Metric, bool) {
	m, ok := ms.Metrics.LoadOrStore(key, metric)
	if !ok {
		// value was stored in ms.Metrics
		metricsActive.Inc()
	}
	return m.(mdata.Metric), ok
}

func (ms *AggMetrics) StoreDataPoint(metric schema.DataPoint, partition int32) {
	m, ok := ms.Metrics.Load(metric.GetId())
	if ok {
		if _, ok = m.(*AggMetric); ok {
			// update the index

		}
		// add datapoint
		m.(mdata.Metric).Add(metric, partition)
		return
	}

	m, _ = ms.LoadOrStore(metric.GetId(), newMetricFromData(metric, partition))
	m.(mdata.Metric).Add(metric, partition)
}

func newMetricFromData(metric schema.DataPoint, partition int32) mdata.Metric {
	md, ok := metric.(*schema.MetricData)

	if ok && md.Interval > 0 {
		// create an AggMetric
		archive := mdata.Idx.AddOrUpdate(md, partition)
		agg := mdata.Aggregations.Get(archive.AggId)
		schema := mdata.Schemas.Get(archive.SchemaId)
		return NewAggMetric(md.Id, schema.Retentions, schema.ReorderWindow, &agg)
	} else {
		//create a MetricBuffer
		return NewMetricBuffer(metric.GetId(), md)
	}
}
