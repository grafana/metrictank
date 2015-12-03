package main

import "fmt"
import "github.com/grafana/grafana/pkg/log"

type aggSetting struct {
	span      uint32 // in seconds, controls how many input points go into an aggregated point.
	chunkSpan uint32 // duration of chunk of aggregated metric for storage, controls how many aggregated points go into 1 chunk
	numChunks uint32 // number of chunks to keep in memory. remember, for a query from now until 3 months ago, we will end up querying the memory server as well.
}

type aggSettingsSpanDesc []aggSetting

func (a aggSettingsSpanDesc) Len() int           { return len(a) }
func (a aggSettingsSpanDesc) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a aggSettingsSpanDesc) Less(i, j int) bool { return a[i].span > a[j].span }

// see description for Aggregator and unit tests
func aggBoundary(ts uint32, span uint32) uint32 {
	return ts + span - ((ts-1)%span + 1)
}

// receives data and builds aggregations
// implementation detail: all points with timestamps t1, t2, t3, t4, t5 get aggregated into a point with ts t5,
// IOW an aggregation point reflects the data in the timeframe preceeding it.
type Aggregator struct {
	key             string // of the metric this aggregator corresponds to
	span            uint32
	currentBoundary uint32 // working on this chunk
	agg             *Aggregation
	minMetric       *AggMetric
	maxMetric       *AggMetric
	sosMetric       *AggMetric
	sumMetric       *AggMetric
	cntMetric       *AggMetric
	lstMetric       *AggMetric
}

func NewAggregator(key string, aggSpan, aggChunkSpan, aggNumChunks uint32) *Aggregator {
	return &Aggregator{
		key:       key,
		span:      aggSpan,
		agg:       NewAggregation(),
		minMetric: NewAggMetric(fmt.Sprintf("%s_min_%d", key, aggSpan), aggChunkSpan, aggNumChunks),
		maxMetric: NewAggMetric(fmt.Sprintf("%s_max_%d", key, aggSpan), aggChunkSpan, aggNumChunks),
		sosMetric: NewAggMetric(fmt.Sprintf("%s_sos_%d", key, aggSpan), aggChunkSpan, aggNumChunks),
		sumMetric: NewAggMetric(fmt.Sprintf("%s_sum_%d", key, aggSpan), aggChunkSpan, aggNumChunks),
		cntMetric: NewAggMetric(fmt.Sprintf("%s_cnt_%d", key, aggSpan), aggChunkSpan, aggNumChunks),
		lstMetric: NewAggMetric(fmt.Sprintf("%s_lst_%d", key, aggSpan), aggChunkSpan, aggNumChunks),
	}
}

func (agg *Aggregator) Add(ts uint32, val float64) {
	boundary := aggBoundary(ts, agg.span)

	if boundary == agg.currentBoundary {
		agg.agg.Add(val)
		log.Debug("aggregator %d Add(): adding to existing block", agg.span)
	} else if boundary > agg.currentBoundary {
		var msg string
		// store current totals as a new point in their series
		// if the cnt is still 0, the numbers are invalid, not to be flushed and we can simply reuse the aggregation
		if agg.agg.cnt != 0 {
			agg.minMetric.Add(agg.currentBoundary, agg.agg.min)
			agg.maxMetric.Add(agg.currentBoundary, agg.agg.max)
			agg.sosMetric.Add(agg.currentBoundary, agg.agg.sos)
			agg.sumMetric.Add(agg.currentBoundary, agg.agg.sum)
			agg.cntMetric.Add(agg.currentBoundary, agg.agg.cnt)
			agg.lstMetric.Add(agg.currentBoundary, agg.agg.lst)
			msg = fmt.Sprintf("flushed cnt %v sum %f min %f max %f , created new aggregation and added new point", agg.span, agg.agg.cnt, agg.agg.sum, agg.agg.min, agg.agg.max)
			agg.agg = NewAggregation()
		} else {
			msg = "added point to existing aggregation"
		}
		agg.currentBoundary = boundary
		agg.agg.Add(val)
		log.Debug("aggregator %d Add(): %s", agg.span, msg)
	} else {
		panic("aggregator: boundary < agg.currentBoundary. ts > lastSeen should already have been asserted")
	}
}
