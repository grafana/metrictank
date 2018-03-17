package mdata

import (
	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/mdata/cache"
	schema "gopkg.in/raintank/schema.v1"
)

// AggBoundary returns ts if it is a boundary, or the next boundary otherwise.
// see description for Aggregator and unit tests, for more details
func AggBoundary(ts uint32, span uint32) uint32 {
	return ts + span - ((ts-1)%span + 1)
}

// receives data and builds aggregations
// note: all points with timestamps t1, t2, t3, t4, [t5] get aggregated into a point with ts t5 where t5 % span = 0.
// in other words:
// * an aggregation point reflects the data in the timeframe preceding it.
// * the timestamps for the aggregated series is quantized to the given span,
// unlike the raw series which may have an offset (be non-quantized)
type Aggregator struct {
	span            uint32
	currentBoundary uint32 // working on this chunk
	agg             *Aggregation
	minMetric       *AggMetric
	maxMetric       *AggMetric
	sumMetric       *AggMetric
	cntMetric       *AggMetric
	lstMetric       *AggMetric
}

func NewAggregator(store Store, cachePusher cache.CachePusher, key schema.AMKey, ret conf.Retention, agg conf.Aggregation, dropFirstChunk bool) *Aggregator {
	if len(agg.AggregationMethod) == 0 {
		panic("NewAggregator called without aggregations. this should never happen")
	}
	span := uint32(ret.SecondsPerPoint)
	aggregator := &Aggregator{
		span: span,
		agg:  NewAggregation(),
	}
	for _, agg := range agg.AggregationMethod {
		switch agg {
		case conf.Avg:
			if aggregator.sumMetric == nil {
				key.Archive = schema.NewArchive(schema.Sum, span)
				aggregator.sumMetric = NewAggMetric(store, cachePusher, key, conf.Retentions{ret}, 0, nil, dropFirstChunk)
			}
			if aggregator.cntMetric == nil {
				key.Archive = schema.NewArchive(schema.Cnt, span)
				aggregator.cntMetric = NewAggMetric(store, cachePusher, key, conf.Retentions{ret}, 0, nil, dropFirstChunk)
			}
		case conf.Sum:
			if aggregator.sumMetric == nil {
				key.Archive = schema.NewArchive(schema.Sum, span)
				aggregator.sumMetric = NewAggMetric(store, cachePusher, key, conf.Retentions{ret}, 0, nil, dropFirstChunk)
			}
		case conf.Lst:
			if aggregator.lstMetric == nil {
				key.Archive = schema.NewArchive(schema.Lst, span)
				aggregator.lstMetric = NewAggMetric(store, cachePusher, key, conf.Retentions{ret}, 0, nil, dropFirstChunk)
			}
		case conf.Max:
			if aggregator.maxMetric == nil {
				key.Archive = schema.NewArchive(schema.Max, span)
				aggregator.maxMetric = NewAggMetric(store, cachePusher, key, conf.Retentions{ret}, 0, nil, dropFirstChunk)
			}
		case conf.Min:
			if aggregator.minMetric == nil {
				key.Archive = schema.NewArchive(schema.Min, span)
				aggregator.minMetric = NewAggMetric(store, cachePusher, key, conf.Retentions{ret}, 0, nil, dropFirstChunk)
			}
		}
	}
	return aggregator
}

// flush adds points to the aggregation-series and resets aggregation state
func (agg *Aggregator) flush() {
	if agg.minMetric != nil {
		agg.minMetric.Add(agg.currentBoundary, agg.agg.Min)
	}
	if agg.maxMetric != nil {
		agg.maxMetric.Add(agg.currentBoundary, agg.agg.Max)
	}
	if agg.sumMetric != nil {
		agg.sumMetric.Add(agg.currentBoundary, agg.agg.Sum)
	}
	if agg.cntMetric != nil {
		agg.cntMetric.Add(agg.currentBoundary, agg.agg.Cnt)
	}
	if agg.lstMetric != nil {
		agg.lstMetric.Add(agg.currentBoundary, agg.agg.Lst)
	}
	//msg := fmt.Sprintf("flushed cnt %v sum %f min %f max %f, reset the block", agg.agg.cnt, agg.agg.sum, agg.agg.min, agg.agg.max)
	agg.agg.Reset()
}

func (agg *Aggregator) Add(ts uint32, val float64) {
	boundary := AggBoundary(ts, agg.span)

	if boundary == agg.currentBoundary {
		agg.agg.Add(val)
		if ts == boundary {
			agg.flush()
		}
	} else if boundary > agg.currentBoundary {
		// store current totals as a new point in their series
		// if the cnt is still 0, the numbers are invalid, not to be flushed and we can simply reuse the aggregation
		if agg.agg.Cnt != 0 {
			agg.flush()
		}
		agg.currentBoundary = boundary
		agg.agg.Add(val)
	} else {
		panic("aggregator: boundary < agg.currentBoundary. ts > lastSeen should already have been asserted")
	}
}

func (agg *Aggregator) GC(now, chunkMinTs, metricMinTs, lastWriteTime uint32) bool {
	ret := true

	if lastWriteTime+agg.span > chunkMinTs {
		// Last datapoint was less than one aggregation window before chunkMinTs, hold out for more data
		return false
	}

	// Haven't seen datapoints in an entire aggregation window before chunkMinTs, time to flush
	if agg.agg.Cnt != 0 {
		agg.flush()
	}

	if agg.minMetric != nil {
		ret = agg.minMetric.GC(now, chunkMinTs, metricMinTs) && ret
	}
	if agg.maxMetric != nil {
		ret = agg.maxMetric.GC(now, chunkMinTs, metricMinTs) && ret
	}
	if agg.sumMetric != nil {
		ret = agg.sumMetric.GC(now, chunkMinTs, metricMinTs) && ret
	}
	if agg.cntMetric != nil {
		ret = agg.cntMetric.GC(now, chunkMinTs, metricMinTs) && ret
	}
	if agg.lstMetric != nil {
		ret = agg.lstMetric.GC(now, chunkMinTs, metricMinTs) && ret
	}

	return ret
}
