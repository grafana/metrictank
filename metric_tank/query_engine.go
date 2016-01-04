package main

import (
	"fmt"
	"github.com/grafana/grafana/pkg/log"
	"sort"
)

// represents a data "archive", i.e. the raw one, or an aggregated series
type archive struct {
	title      string
	interval   uint32
	pointCount uint32
	comment    string
}

func (b archive) String() string {
	return fmt.Sprintf("<archive %s> int:%d, comment: %s", b.title, b.interval, b.comment)
}

type archives []archive

func (a archives) Len() int           { return len(a) }
func (a archives) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a archives) Less(i, j int) bool { return a[i].interval < a[j].interval }

func findMetricsForRequests(reqs []Req, metaCache *MetaCache) error {
	for i := range reqs {
		err := metaCache.UpdateReq(&reqs[i])
		if err != nil {
			return err
		}
	}
	return nil
}

// updates the requests with all details for fetching, making sure all metrics are in the same, optimal interval
// luckily, all metrics still use the same aggSettings, making this a bit simpler
// for all requests, sets archive, numPoints, interval (and rawInterval as a side effect)
// note: it is assumed that all requests have the same from, to and maxdatapoints!
func alignRequests(reqs []Req, aggSettings []aggSetting) ([]Req, error) {

	// model all the archives for each requested metric
	// the 0th archive is always the raw series, with highest res (lowest interval)
	aggs := aggSettingsSpanAsc(aggSettings)
	sort.Sort(aggs)

	options := make([]archive, len(aggs)+1)

	minInterval := uint32(0)
	rawIntervals := make(map[uint32]bool)
	for _, req := range reqs {
		if minInterval == 0 || minInterval > req.rawInterval {
			minInterval = req.rawInterval
		}
		rawIntervals[req.rawInterval] = true
	}
	tsRange := (reqs[0].to - reqs[0].from)

	options[0] = archive{"raw", minInterval, tsRange / minInterval, ""}
	// now model the archives we get from the aggregations
	for j, agg := range aggs {
		options[j+1] = archive{fmt.Sprintf("agg %d", j), agg.span, tsRange / agg.span, ""}
	}

	// find the first option with a pointCount < maxDataPoints
	selected := len(options) - 1
	for i, opt := range options {
		if opt.pointCount < reqs[0].maxPoints {
			selected = i
			break
		}
	}

	/*
	   do a quick calculation of the ratio between pointCount and maxDatapoints of
	   the selected option, and the option before that.  eg. with a time range of 1hour,
	   our pointCounts for each option are:
	   10s   | 360
	   600s  | 6
	   7200s | 0

	   if maxPoints is 100, then selected will be 1, our 600s rollups.
	   We then calculate the ratio between maxPoints and our
	   selected pointCount "6" and the previous option "360".
	   belowMaxDataPointsRatio  =  16  #(100/6)
	   aboveMaxDataPointsRatio  =  3   #(360/100)

	   As the maxDataPoint requested is much closer to 360 then it is to 6,
	   we will use 360 and do runtime consolidation.
	*/
	runTimeConsolidate := false
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
		var keys []int
		for k := range rawIntervals {
			keys = append(keys, int(k))
		}
		sort.Ints(keys)
		chosenInterval = uint32(keys[0])
		for i := 1; i < len(keys); i++ {
			var a, b uint32
			if uint32(keys[i]) > chosenInterval {
				a = uint32(keys[i])
				b = chosenInterval
			} else {
				a = chosenInterval
				b = uint32(keys[i])
			}
			r := a % b
			if r != 0 {
				for j := uint32(2); j <= b; j++ {
					if (j*a)%b == 0 {
						chosenInterval = j * a
						break
					}
				}
			} else {

				chosenInterval = uint32(a)
			}
			options[0].pointCount = tsRange / chosenInterval
			options[0].interval = chosenInterval
		}
		//make sure that the calculated interval is not greater then the interval of the fist rollup.
		if len(options) > 1 && chosenInterval > options[1].interval {
			selected = 1
			chosenInterval = options[1].interval
		}
	}

	options[selected].comment = "<-- chosen"
	for _, archive := range options {
		log.Debug("%-6s %-6d %-6d %s", archive.title, archive.interval, tsRange/archive.interval, archive.comment)
	}

	/* we now just need to update the archiveInterval, outInterval and aggNum of each req.
	   archInterval uint32 // the interval corresponding to the archive we'll fetch
	   outInterval  uint32 // the interval of the output data, after any runtime consolidation
	   aggNum       uint32 // how many points to consolidate together at runtime, after fetching from the archive
	*/
	for i, _ := range reqs {
		req := &reqs[i]
		req.archive = selected
		req.archInterval = options[selected].interval
		req.outInterval = chosenInterval
		aggNum := uint32(1)
		if runTimeConsolidate {
			ptCount := options[selected].pointCount

			aggNum = ptCount / req.maxPoints
			if ptCount%req.maxPoints != 0 {
				aggNum++
			}
			if selected == 0 {
				// Handle RAW interval
				req.archInterval = req.rawInterval

				// each request can have a different rawInterval. So the aggNum is vairable.
				if chosenInterval != req.rawInterval {
					aggNum = aggNum * (chosenInterval / req.rawInterval)
				}
			}

			req.outInterval = req.archInterval * aggNum
		}

		req.aggNum = aggNum
	}
	return reqs, nil
}
