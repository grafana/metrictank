package mdata

import (
	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/consolidation"
	"github.com/grafana/metrictank/mdata/cache"
	"github.com/grafana/metrictank/schema"
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

func NewAggregator(store Store, cachePusher cache.CachePusher, key schema.AMKey, retOrig string, ret conf.Retention, agg conf.Aggregation, dropFirstChunk bool, ingestFrom int64) *Aggregator {
	if len(agg.AggregationMethod) == 0 {
		panic("NewAggregator called without aggregations. this should never happen")
	}
	span := uint32(ret.SecondsPerPoint)
	aggregator := &Aggregator{
		span: span,
		agg:  NewAggregation(),
	}
	for _, agg := range agg.AggregationMethod {
		retentions := conf.Retentions{
			Orig: retOrig,
			Rets: []conf.Retention{ret},
		}
		switch agg {
		case conf.Avg:
			if aggregator.sumMetric == nil {
				key.Archive = schema.NewArchive(schema.Sum, span)
				aggregator.sumMetric = NewAggMetric(store, cachePusher, key, retentions, 0, span, nil, false, dropFirstChunk, ingestFrom)
			}
			if aggregator.cntMetric == nil {
				key.Archive = schema.NewArchive(schema.Cnt, span)
				aggregator.cntMetric = NewAggMetric(store, cachePusher, key, retentions, 0, span, nil, false, dropFirstChunk, ingestFrom)
			}
		case conf.Sum:
			if aggregator.sumMetric == nil {
				key.Archive = schema.NewArchive(schema.Sum, span)
				aggregator.sumMetric = NewAggMetric(store, cachePusher, key, retentions, 0, span, nil, false, dropFirstChunk, ingestFrom)
			}
		case conf.Lst:
			if aggregator.lstMetric == nil {
				key.Archive = schema.NewArchive(schema.Lst, span)
				aggregator.lstMetric = NewAggMetric(store, cachePusher, key, retentions, 0, span, nil, false, dropFirstChunk, ingestFrom)
			}
		case conf.Max:
			if aggregator.maxMetric == nil {
				key.Archive = schema.NewArchive(schema.Max, span)
				aggregator.maxMetric = NewAggMetric(store, cachePusher, key, retentions, 0, span, nil, false, dropFirstChunk, ingestFrom)
			}
		case conf.Min:
			if aggregator.minMetric == nil {
				key.Archive = schema.NewArchive(schema.Min, span)
				aggregator.minMetric = NewAggMetric(store, cachePusher, key, retentions, 0, span, nil, false, dropFirstChunk, ingestFrom)
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

// Foresee duplicates the underlying Aggregation,
// consumes points in the future, and return the newly aggregated points.
// Foresee won't actually add those points to AggMetric.
func (agg *Aggregator) Foresee(consolidator consolidation.Consolidator, from, to uint32, futurePoints []schema.Point) []schema.Point {
	aggregationPreview := *(agg.agg)
	var aggregatedPoints []schema.Point
	currentBoundary := agg.currentBoundary
	for _, point := range futurePoints {
		boundary := AggBoundary(point.Ts, agg.span)
		if boundary < currentBoundary {
			// ignore the point it was for a previous bucket. we can't process it
			continue
		} else if boundary > currentBoundary {
			// point is for a more recent bucket
			// store current aggregates as a new point in their series and start the new bucket
			// if the cnt is still 0, the numbers are invalid, not to be flushed and we can simply reuse the aggregation
			if aggregationPreview.Cnt != 0 {
				value, _ := aggregationPreview.GetValueFor(consolidator)
				aggregatedPoints = append(aggregatedPoints, schema.Point{Val: value, Ts: currentBoundary})
				aggregationPreview.Reset()
			}
			currentBoundary = boundary
		}
		aggregationPreview.Add(point.Val)
	}
	if aggregationPreview.Cnt != 0 && currentBoundary <= to {
		value, _ := aggregationPreview.GetValueFor(consolidator)
		aggregatedPoints = append(aggregatedPoints, schema.Point{Val: value, Ts: currentBoundary})
	}
	return aggregatedPoints
}

// Add adds the point to the in-progress aggregation, and flushes it if we reached the boundary
// points going back in time are accepted, unless they go into a previous bucket, in which case they are ignored
func (agg *Aggregator) Add(ts uint32, val float64) {
	boundary := AggBoundary(ts, agg.span)

	if boundary < agg.currentBoundary {
		// ignore the point it was for a previous bucket. we can't process it
		return
	} else if boundary > agg.currentBoundary {
		// point is for a more recent bucket
		// store current aggregates as a new point in their series and start the new bucket
		// if the cnt is still 0, the numbers are invalid, not to be flushed and we can simply reuse the aggregation
		if agg.agg.Cnt != 0 {
			agg.flush()
		}
		agg.currentBoundary = boundary
	}
	agg.agg.Add(val)

	// if the ts of the point is a boundary, it means no more point can possibly come in for the same aggregation.
	// e.g. if aggspan is 10s and we're adding a point with timestamp 12:34:10, then any subsequent point will go
	// in the bucket for 12:34:20 or later.
	// so it is time to flush the result
	if ts == boundary {
		agg.flush()
	}
}

// GC returns whether all of the associated series are stale and can be removed, and their combined pointcount if so
func (agg *Aggregator) GC(now, chunkMinTs, metricMinTs, lastWriteTime uint32) (uint32, bool) {
	var points uint32
	stale := true

	if lastWriteTime+agg.span > chunkMinTs {
		// Last datapoint was less than one aggregation window before chunkMinTs, hold out for more data
		return 0, false
	}

	// Haven't seen datapoints in an entire aggregation window before chunkMinTs, time to flush
	if agg.agg.Cnt != 0 {
		agg.flush()
	}

	if agg.minMetric != nil {
		p, s := agg.minMetric.GC(now, chunkMinTs, metricMinTs)
		stale = stale && s
		points += p

	}
	if agg.maxMetric != nil {
		p, s := agg.maxMetric.GC(now, chunkMinTs, metricMinTs)
		stale = stale && s
		points += p
	}
	if agg.sumMetric != nil {
		p, s := agg.sumMetric.GC(now, chunkMinTs, metricMinTs)
		stale = stale && s
		points += p
	}
	if agg.cntMetric != nil {
		p, s := agg.cntMetric.GC(now, chunkMinTs, metricMinTs)
		stale = stale && s
		points += p
	}
	if agg.lstMetric != nil {
		p, s := agg.lstMetric.GC(now, chunkMinTs, metricMinTs)
		stale = stale && s
		points += p
	}

	return points, stale
}
