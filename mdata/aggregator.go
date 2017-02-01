package mdata

import (
	"fmt"
	"github.com/raintank/metrictank/mdata/cache"
)

type AggSetting struct {
	Span      uint32 // in seconds, controls how many input points go into an aggregated point.
	ChunkSpan uint32 // duration of chunk of aggregated metric for storage, controls how many aggregated points go into 1 chunk
	NumChunks uint32 // number of chunks to keep in memory. remember, for a query from now until 3 months ago, we will end up querying the memory server as well.
	TTL       uint32 // how many seconds to keep the chunk in cassandra
	Ready     bool   // ready for reads?
}

type AggSettings struct {
	RawTTL uint32       // TTL for raw data
	Aggs   []AggSetting // aggregations
}

func NewAggSetting(span, chunkSpan, numChunks, ttl uint32, ready bool) AggSetting {
	return AggSetting{
		Span:      span,
		ChunkSpan: chunkSpan,
		NumChunks: numChunks,
		TTL:       ttl,
		Ready:     ready,
	}
}

type AggSettingsSpanAsc []AggSetting

func (a AggSettingsSpanAsc) Len() int           { return len(a) }
func (a AggSettingsSpanAsc) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a AggSettingsSpanAsc) Less(i, j int) bool { return a[i].Span < a[j].Span }

// aggBoundary returns ts if it is a boundary, or the next boundary otherwise.
// see description for Aggregator and unit tests, for more details
func aggBoundary(ts uint32, span uint32) uint32 {
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
}

func NewAggregator(store Store, cachePusher cache.CachePusher, key string, aggSpan, aggChunkSpan, aggNumChunks uint32, ttl uint32) *Aggregator {
	return &Aggregator{
		key:       key,
		span:      aggSpan,
		agg:       NewAggregation(),
		minMetric: NewAggMetric(store, cachePusher, fmt.Sprintf("%s_min_%d", key, aggSpan), aggChunkSpan, aggNumChunks, ttl),
		maxMetric: NewAggMetric(store, cachePusher, fmt.Sprintf("%s_max_%d", key, aggSpan), aggChunkSpan, aggNumChunks, ttl),
		sumMetric: NewAggMetric(store, cachePusher, fmt.Sprintf("%s_sum_%d", key, aggSpan), aggChunkSpan, aggNumChunks, ttl),
		cntMetric: NewAggMetric(store, cachePusher, fmt.Sprintf("%s_cnt_%d", key, aggSpan), aggChunkSpan, aggNumChunks, ttl),
	}
}
func (agg *Aggregator) flush() {
	agg.minMetric.Add(agg.currentBoundary, agg.agg.min)
	agg.maxMetric.Add(agg.currentBoundary, agg.agg.max)
	agg.sumMetric.Add(agg.currentBoundary, agg.agg.sum)
	agg.cntMetric.Add(agg.currentBoundary, agg.agg.cnt)
	//msg := fmt.Sprintf("flushed cnt %v sum %f min %f max %f, reset the block", agg.agg.cnt, agg.agg.sum, agg.agg.min, agg.agg.max)
	agg.agg.Reset()
}

func (agg *Aggregator) Add(ts uint32, val float64) {
	boundary := aggBoundary(ts, agg.span)

	if boundary == agg.currentBoundary {
		agg.agg.Add(val)
		if ts == boundary {
			agg.flush()
			//log.Debug("aggregator %d Add(): added to aggregation block, %s because this was the last point for the block", agg.span, agg.flush())
			//} else {
			//		log.Debug("aggregator %d Add(): added to aggregation block", agg.span)
			//	}
		}
	} else if boundary > agg.currentBoundary {
		// store current totals as a new point in their series
		// if the cnt is still 0, the numbers are invalid, not to be flushed and we can simply reuse the aggregation
		if agg.agg.cnt != 0 {
			agg.flush()
			//		msg = agg.flush() + "and added new point"
			//	} else {
			//		msg = "added point to still-unused aggregation block"
		}
		agg.currentBoundary = boundary
		agg.agg.Add(val)
		//	log.Debug("aggregator %d Add(): %s", agg.span, msg)
	} else {
		panic("aggregator: boundary < agg.currentBoundary. ts > lastSeen should already have been asserted")
	}
}
