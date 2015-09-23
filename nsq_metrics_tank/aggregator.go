package main

import "fmt"

// aggregates data.
// implementation detail: all timestamps t1, t2, t3, t4, t5 get aggregated into a point with ts t5.
type Aggregator struct {
	chunkSpan    uint32
	lastBoundary uint32 // of successfull completion
}

func (agg *Aggregator) Signal(a *AggMetric, ts uint32) {
	boundary := ts - (ts % a.chunkSpan)
	if boundary > agg.lastBoundary {
		iters := a.Get(agg.lastBoundary+1, boundary+1)
		fmt.Println("aggregating data from", agg.lastBoundary, "to", boundary+1)
		for _, iter := range iters {
			for iter.Next() {
				fmt.Println(iter.Values())
			}
		}
		agg.lastBoundary = boundary
	}
}
