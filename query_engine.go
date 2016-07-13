package main

import (
	"fmt"
	"sort"

	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/worldping-api/pkg/log"
)

// represents a data "archive", i.e. the raw one, or an aggregated series
type archive struct {
	interval   uint32
	pointCount uint32
	chosen     bool
}

func (b archive) String() string {
	return fmt.Sprintf("<archive int:%d, pointCount: %d, chosen: %t", b.interval, b.pointCount, b.chosen)
}

type archives []archive

func (a archives) Len() int           { return len(a) }
func (a archives) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a archives) Less(i, j int) bool { return a[i].interval < a[j].interval }

// updates the requests with all details for fetching, making sure all metrics are in the same, optimal interval
// luckily, all metrics still use the same aggSettings, making this a bit simpler
// note: it is assumed that all requests have the same from, to and maxdatapoints!
// this function ignores the TTL values. it is assumed that you've set sensible TTL's
func alignRequests(reqs []Req, aggSettings []mdata.AggSetting) ([]Req, error) {

	// model all the archives for each requested metric
	// the 0th archive is always the raw series, with highest res (lowest interval)
	aggs := mdata.AggSettingsSpanAsc(aggSettings)
	sort.Sort(aggs)

	options := make([]archive, 1, len(aggs)+1)

	minInterval := uint32(0) // will contain the smallest rawInterval from all requested series
	rawIntervals := make(map[uint32]struct{})
	for _, req := range reqs {
		if minInterval == 0 || minInterval > req.rawInterval {
			minInterval = req.rawInterval
		}
		rawIntervals[req.rawInterval] = struct{}{}
	}
	tsRange := (reqs[0].to - reqs[0].from)

	// note: not all series necessarily have the same raw settings, will be fixed further down
	options[0] = archive{minInterval, tsRange / minInterval, false}
	// now model the archives we get from the aggregations
	// note that during the processing, we skip non-ready aggregations for simplicity, but at the
	// end we need to convert the index back to the real index in the full (incl non-ready) aggSettings array.
	aggRef := []int{0}
	for j, agg := range aggs {
		if agg.Ready {
			options = append(options, archive{agg.Span, tsRange / agg.Span, false})
			aggRef = append(aggRef, j+1)
		}
	}

	// find the first, i.e. highest-res option with a pointCount <= maxDataPoints
	// if all options have too many points, fall back to the lowest-res option and apply runtime
	// consolidation
	selected := len(options) - 1
	runTimeConsolidate := true
	for i, opt := range options {
		if opt.pointCount <= reqs[0].maxPoints {
			runTimeConsolidate = false
			selected = i
			break
		}
	}

	/*
	   do a quick calculation of the ratio between pointCount and maxDatapoints of
	   the selected option, and the option before that; if the previous option is
	   a lot closer to max points than we are, we pick that and apply some runtime
	   consolidation.
	   eg. with a time range of 1hour,
	   our options are:
	   i | span  | pointCount
	   ======================
	   0 | 10s   | 360
	   1 | 600s  | 6
	   2 | 7200s | 0

	   if maxPoints is 100, then selected will be 1, our 600s rollups.
	   We then calculate the ratio between maxPoints and our
	   selected pointCount "6" and the previous option "360".
	   belowMaxDataPointsRatio = 100/6   = 16.67
	   aboveMaxDataPointsRatio = 360/100 = 3.6

	   As the maxDataPoint requested is much closer to 360 then it is to 6,
	   we will use 360 and do runtime consolidation.
	*/
	if selected > 0 {
		belowMaxDataPointsRatio := float64(reqs[0].maxPoints) / float64(options[selected].pointCount)
		aboveMaxDataPointsRatio := float64(options[selected-1].pointCount) / float64(reqs[0].maxPoints)

		if aboveMaxDataPointsRatio < belowMaxDataPointsRatio {
			selected--
			runTimeConsolidate = true
		}
	}

	chosenInterval := options[selected].interval

	// if we are using raw metrics, we need to find an interval that all request intervals work with.
	if selected == 0 && len(rawIntervals) > 1 {
		runTimeConsolidate = true
		var keys []uint32
		for k := range rawIntervals {
			keys = append(keys, k)
		}
		chosenInterval = lcm(keys)
		options[0].interval = chosenInterval
		options[0].pointCount = tsRange / chosenInterval
		//make sure that the calculated interval is not greater then the interval of the first rollup.
		if len(options) > 1 && chosenInterval >= options[1].interval {
			selected = 1
			chosenInterval = options[1].interval
		}
	}

	if logLevel < 2 {
		options[selected].chosen = true
		for i, archive := range options {
			if archive.chosen {
				log.Debug("QE %-2d %-6d %-6d <-", i, archive.interval, tsRange/archive.interval)
			} else {
				log.Debug("QE %-2d %-6d %-6d", i, archive.interval, tsRange/archive.interval)
			}
		}
	}

	/* we now just need to update the following properties for each req:
	   archive      int    // 0 means original data, 1 means first agg level, 2 means 2nd, etc.
	   archInterval uint32 // the interval corresponding to the archive we'll fetch
	   outInterval  uint32 // the interval of the output data, after any runtime consolidation
	   aggNum       uint32 // how many points to consolidate together at runtime, after fetching from the archive
	*/
	for i := range reqs {
		req := &reqs[i]
		req.archive = aggRef[selected]
		req.archInterval = options[selected].interval
		req.outInterval = chosenInterval
		req.aggNum = 1
		if runTimeConsolidate {
			req.aggNum = aggEvery(options[selected].pointCount, req.maxPoints)

			// options[0].{interval,pointCount} didn't necessarily reflect the actual raw archive for this request,
			// so adjust where needed.
			if selected == 0 && chosenInterval != req.rawInterval {
				req.archInterval = req.rawInterval
				req.aggNum *= chosenInterval / req.rawInterval
			}

			req.outInterval = req.archInterval * req.aggNum
		}
	}
	return reqs, nil
}
