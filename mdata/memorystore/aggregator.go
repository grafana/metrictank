package memorystore

import (
	"fmt"

	"github.com/grafana/metrictank/conf"
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
	key             string // of the metric this aggregator corresponds to
	span            uint32
	currentBoundary uint32 // working on this chunk
	agg             *Aggregation
	minMetric       *AggMetric
	maxMetric       *AggMetric
	sumMetric       *AggMetric
	cntMetric       *AggMetric
	lstMetric       *AggMetric
}

func NewAggregator(key string, ret conf.Retention, agg conf.Aggregation) *Aggregator {
	if len(agg.AggregationMethod) == 0 {
		panic("NewAggregator called without aggregations. this should never happen")
	}
	span := uint32(ret.SecondsPerPoint)
	aggregator := &Aggregator{
		key:  key,
		span: span,
		agg:  NewAggregation(),
	}
	for _, agg := range agg.AggregationMethod {
		switch agg {
		case conf.Avg:
			if aggregator.sumMetric == nil {
				aggregator.sumMetric = NewAggMetric(fmt.Sprintf("%s_sum_%d", key, span), conf.Retentions{ret}, 0, nil)
			}
			if aggregator.cntMetric == nil {
				aggregator.cntMetric = NewAggMetric(fmt.Sprintf("%s_cnt_%d", key, span), conf.Retentions{ret}, 0, nil)
			}
		case conf.Sum:
			if aggregator.sumMetric == nil {
				aggregator.sumMetric = NewAggMetric(fmt.Sprintf("%s_sum_%d", key, span), conf.Retentions{ret}, 0, nil)
			}
		case conf.Lst:
			if aggregator.lstMetric == nil {
				aggregator.lstMetric = NewAggMetric(fmt.Sprintf("%s_lst_%d", key, span), conf.Retentions{ret}, 0, nil)
			}
		case conf.Max:
			if aggregator.maxMetric == nil {
				aggregator.maxMetric = NewAggMetric(fmt.Sprintf("%s_max_%d", key, span), conf.Retentions{ret}, 0, nil)
			}
		case conf.Min:
			if aggregator.minMetric == nil {
				aggregator.minMetric = NewAggMetric(fmt.Sprintf("%s_min_%d", key, span), conf.Retentions{ret}, 0, nil)
			}
		}
	}
	return aggregator
}

// flush adds points to the aggregation-series and resets aggregation state
func (agg *Aggregator) flush() {
	if agg.minMetric != nil {
		agg.minMetric.AddPoint(agg.currentBoundary, agg.agg.Min)
	}
	if agg.maxMetric != nil {
		agg.maxMetric.AddPoint(agg.currentBoundary, agg.agg.Max)
	}
	if agg.sumMetric != nil {
		agg.sumMetric.AddPoint(agg.currentBoundary, agg.agg.Sum)
	}
	if agg.cntMetric != nil {
		agg.cntMetric.AddPoint(agg.currentBoundary, agg.agg.Cnt)
	}
	if agg.lstMetric != nil {
		agg.lstMetric.AddPoint(agg.currentBoundary, agg.agg.Lst)
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
