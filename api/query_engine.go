package api

import (
	"fmt"
	"time"

	"github.com/raintank/metrictank/api/models"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/stats"
	"github.com/raintank/metrictank/util"
	"github.com/raintank/worldping-api/pkg/log"
)

var (
	lowResNumPointsFetch  = stats.NewMeter32("api.align_requests.low_res.num_points_fetching", false)
	lowResNumPointsReturn = stats.NewMeter32("api.align_requests.low_res.num_points_returning", false)
	lowResChosenArchive   = stats.NewMeter32("api.align_requests.low_res.chosen_archive", false)
	maxResNumPoints       = stats.NewMeter32("api.align_requests.max_res.num_points", false)
	maxResChosenArchive   = stats.NewMeter32("api.align_requests.max_res.chosen_archive", false)
)

// represents a data "archive", i.e. the raw one, or an aggregated series
type archive struct {
	interval   uint32
	pointCount uint32
	chosen     bool
	ttl        uint32
}

func (b archive) String() string {
	return fmt.Sprintf("<archive int:%d, pointCount: %d, chosen: %t", b.interval, b.pointCount, b.chosen)
}

type archives []archive

func (a archives) Len() int           { return len(a) }
func (a archives) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a archives) Less(i, j int) bool { return a[i].interval < a[j].interval }

// summarizeRawIntervals returns the set of all rawIntervals seen, as well as the minimum value
func summarizeRawIntervals(reqs []models.Req) (uint32, map[uint32]struct{}) {
	min := uint32(0)
	all := make(map[uint32]struct{})
	for _, req := range reqs {
		if min == 0 || min > req.RawInterval {
			min = req.RawInterval
		}
		all[req.RawInterval] = struct{}{}
	}
	return min, all
}

// getOptions returns a slice describing each readable archive, as well as a lookup slice
// which, given an index for a given archive, will return the index in the sorted list of all archives (incl raw)
// IMPORTANT: not all series necessarily have the same raw settings, but we only return the smallest one.
// this is corrected further down.
func getOptions(aggSettings mdata.AggSettings, minInterval, tsRange uint32) ([]archive, []int) {
	// model all the archives for each requested metric
	// the 0th archive is always the raw series, with highest res (lowest interval)
	options := make([]archive, 1, len(aggSettings.Aggs)+1)

	options[0] = archive{minInterval, tsRange / minInterval, false, aggSettings.RawTTL}
	aggRef := []int{0}
	// now model the archives we get from the aggregations
	// note that during the processing, we skip non-ready aggregations for simplicity, but at the
	// end we need to convert the index back to the real index in the full (incl non-ready) aggSettings array.
	for j, agg := range aggSettings.Aggs {
		if agg.Ready {
			options = append(options, archive{agg.Span, tsRange / agg.Span, false, agg.TTL})
			aggRef = append(aggRef, j+1)
		}
	}
	return options, aggRef
}

// updates the requests with all details for fetching, making sure all metrics are in the same, optimal interval
// luckily, all metrics still use the same aggSettings, making this a bit simpler
// note: it is assumed that all requests have the same from, to and maxdatapoints!
// this function ignores the TTL values. it is assumed that you've set sensible TTL's
func alignRequests(reqs []models.Req, aggSettings mdata.AggSettings) ([]models.Req, error) {

	tsRange := (reqs[0].To - reqs[0].From)
	minInterval, rawIntervals := summarizeRawIntervals(reqs)
	options, aggRef := getOptions(aggSettings, minInterval, tsRange)

	// find the first, i.e. highest-res option with a pointCount <= maxDataPoints
	// if all options have too many points, fall back to the lowest-res option and apply runtime
	// consolidation
	selected := len(options) - 1
	runTimeConsolidate := true
	for i, opt := range options {
		if opt.pointCount <= reqs[0].MaxPoints {
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
		belowMaxDataPointsRatio := float64(reqs[0].MaxPoints) / float64(options[selected].pointCount)
		aboveMaxDataPointsRatio := float64(options[selected-1].pointCount) / float64(reqs[0].MaxPoints)

		if aboveMaxDataPointsRatio < belowMaxDataPointsRatio {
			selected--
			runTimeConsolidate = true
		}
	}

	chosenInterval := options[selected].interval

	// if we are using raw metrics, we need to find an interval that all request intervals work with.
	if selected == 0 && len(rawIntervals) > 1 {
		runTimeConsolidate = true
		keys := make([]uint32, len(rawIntervals))
		i := 0
		for k := range rawIntervals {
			keys[i] = k
			i++
		}
		chosenInterval = util.Lcm(keys)
		options[0].interval = chosenInterval
		options[0].pointCount = tsRange / chosenInterval
		//make sure that the calculated interval is not greater then the interval of the first rollup.
		if len(options) > 1 && chosenInterval >= options[1].interval {
			selected = 1
			chosenInterval = options[1].interval
		}
	}

	if LogLevel < 2 {
		options[selected].chosen = true
		for i, archive := range options {
			if archive.chosen {
				log.Debug("QE %-2d %-6d %-6d <-", i, archive.interval, tsRange/archive.interval)
			} else {
				log.Debug("QE %-2d %-6d %-6d", i, archive.interval, tsRange/archive.interval)
			}
		}
	}

	// === intermezzo ===
	// prospective new approach, per https://github.com/raintank/metrictank/issues/463 :
	// find the highest resolution archive that has enough retention.
	// not in use yet. but report metrics on what would be.

	// find the highest res archive that retains all the data we need.
	// fallback to lowest res option (which *should* have the longest TTL)
	selectedMaxRes := len(options) - 1
	now := uint32(time.Now().Unix())
	for i := len(options) - 2; i >= 0; i-- {
		if now-options[i].ttl > reqs[0].From {
			break
		}
		selectedMaxRes = i
	}

	// === end intermezzo ===

	/* we now just need to update the following properties for each req:
	   archive      int    // 0 means original data, 1 means first agg level, 2 means 2nd, etc.
	   archInterval uint32 // the interval corresponding to the archive we'll fetch
	   outInterval  uint32 // the interval of the output data, after any runtime consolidation
	   aggNum       uint32 // how many points to consolidate together at runtime, after fetching from the archive
	*/
	for i := range reqs {
		req := &reqs[i]
		req.Archive = aggRef[selected]
		req.ArchInterval = options[selected].interval
		req.TTL = options[selected].ttl
		req.OutInterval = chosenInterval
		req.AggNum = 1
		pointCount := options[selected].pointCount
		if runTimeConsolidate {
			req.AggNum = aggEvery(options[selected].pointCount, req.MaxPoints)

			// options[0].{interval,pointCount} didn't necessarily reflect the actual raw archive for this request,
			// so adjust where needed.
			if selected == 0 && chosenInterval != req.RawInterval {
				req.ArchInterval = req.RawInterval
				req.AggNum *= chosenInterval / req.RawInterval
				pointCount = tsRange / req.ArchInterval
			}

			req.OutInterval = req.ArchInterval * req.AggNum
		}

		lowResNumPointsFetch.ValuesUint32(pointCount, uint32(len(reqs)))
		lowResNumPointsReturn.ValueUint32(tsRange / req.OutInterval)

		pointCountMaxRes := options[selectedMaxRes].pointCount
		// just like higher up, the value may need to be adjusted
		if selectedMaxRes == 0 {
			pointCountMaxRes = tsRange / req.RawInterval
		}
		maxResNumPoints.ValuesUint32(pointCountMaxRes, uint32(len(reqs)))
	}

	lowResChosenArchive.Values(aggRef[selected], len(reqs))
	maxResChosenArchive.Values(aggRef[selectedMaxRes], len(reqs))

	return reqs, nil
}
