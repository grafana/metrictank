package main

import "fmt"

type aggSetting struct {
	span      uint32 // in seconds
	chunkSpan uint32 // duration of chunk of aggregated metric for storage
	numChunks uint32 // number of chunks to keep in memory // TODO: we probably won't ever query them in memory, so this would become a concern of the persisting code
}

// see description for Aggregator and unit tests
func aggBoundary(ts uint32, span uint32) uint32 {
	return ts + span - ((ts-1)%span + 1)
}

// receives data and builds aggregations
// implementation detail: all timestamps t1, t2, t3, t4, t5 get aggregated into a point with ts t5.
type Aggregator struct {
	key             string
	span            uint32
	currentBoundary uint32 // working on this chunk
	agg             *Aggregation
	minMetric       *AggMetric
	maxMetric       *AggMetric
	sosMetric       *AggMetric
	sumMetric       *AggMetric
	cntMetric       *AggMetric
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
	}
}

func (agg *Aggregator) Add(ts uint32, val float64) {
	boundary := aggBoundary(ts, agg.span)

	if boundary > agg.currentBoundary {
		agg.agg.Flush(agg.currentBoundary)
		agg.agg = NewAggregation()
		agg.currentBoundary = boundary
	} else {
		agg.agg.Add(ts, val)
	}
}
