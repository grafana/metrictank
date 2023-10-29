package mdata

import (
	"testing"
	"time"

	"github.com/grafana/metrictank/internal/mdata/cache"
	"github.com/grafana/metrictank/internal/mdata/chunk"
	"github.com/grafana/metrictank/internal/schema"
	"github.com/grafana/metrictank/pkg/conf"
)

type mockCachePusher struct{}

func (m *mockCachePusher) IsCacheable(_ schema.AMKey) bool {
	return true
}

func (m *mockCachePusher) AddIfHot(_ schema.AMKey, _ uint32, _ chunk.IterGen) {}

func NewMockCachePusher() cache.CachePusher {
	return &mockCachePusher{}
}

func TestAggMetricsGetOrCreate(t *testing.T) {
	mockStore := NewMockStore()
	mockCachePusher := NewMockCachePusher()
	ingestFrom := make(map[uint32]int64)
	chunkMaxStale := uint32(60)
	metricMaxStale := uint32(120)
	gcInterval := time.Hour

	_futureToleranceRatio := futureToleranceRatio
	_aggregations := Aggregations
	_schemas := Schemas
	defer func() {
		futureToleranceRatio = _futureToleranceRatio
		Aggregations = _aggregations
		Schemas = _schemas
	}()

	futureToleranceRatio = 50
	Aggregations = conf.NewAggregations()
	Schemas = conf.NewSchemas([]conf.Schema{{
		Name: "schema1",
		Retentions: conf.Retentions{
			Rets: []conf.Retention{
				{
					SecondsPerPoint: 10,
					NumberOfPoints:  360 * 24,
					ChunkSpan:       600,
					NumChunks:       2,
					Ready:           0,
				}, {
					SecondsPerPoint: 3600,
					NumberOfPoints:  24 * 365,
					ChunkSpan:       24 * 3600,
					NumChunks:       2,
					Ready:           0,
				}},
		},
	}})

	aggMetrics := NewAggMetrics(mockStore, mockCachePusher, false, ingestFrom, chunkMaxStale, metricMaxStale, gcInterval)

	testKey1, _ := schema.AMKeyFromString("1.12345678901234567890123456789012")
	metric := aggMetrics.GetOrCreate(testKey1.MKey, 1, 0, 10).(*AggMetric)

	if metric.flusher.(*AggMetrics) != aggMetrics {
		t.Fatalf("Expected metric to have flusher referencing aggMetrics, but it did not")
	}

	if metric.key.MKey != testKey1.MKey {
		t.Fatalf("Expected metric to have test metric key, but it did not")
	}

	if metric.chunkSpan != 24*3600 {
		t.Fatalf("Expected metric chunk span to be %d, but it was %d", 24*3600, metric.chunkSpan)
	}

	if cap(metric.chunks) != 2 {
		t.Fatalf("Expected metric num chunks to be 2, but it was %d", cap(metric.chunks))
	}

	if metric.ttl != 3600*24*365 {
		t.Fatalf("Expected metric ttl to be %d, but it was %d", 3600*24*365, metric.ttl)
	}

	// storage schema's maxTTL is 1 year, future tolerance ratio is 50, so our future tolerance should be 1/2 year
	expectedFutureTolerance := uint32(3600 * 24 * 365 * futureToleranceRatio / 100)
	if metric.futureTolerance != expectedFutureTolerance {
		t.Fatalf("Expected future tolerance to be %d, was %d", expectedFutureTolerance, metric.futureTolerance)
	}

	// verify that two calls to GetOrCreate with the same parameters return the same struct
	metric2 := aggMetrics.GetOrCreate(testKey1.MKey, 1, 0, 10).(*AggMetric)
	if metric != metric2 {
		t.Fatalf("Expected GetOrCreate to return twice the same metric for the same key")
	}

	futureToleranceRatio = 0
	testKey2, _ := schema.AMKeyFromString("1.12345678901234567890123456789013")
	metric3 := aggMetrics.GetOrCreate(testKey2.MKey, 1, 0, 10).(*AggMetric)
	if metric3.futureTolerance != 0 {
		t.Fatalf("Future tolerance was expected to be 0, but it was %d", metric3.futureTolerance)
	}
}
