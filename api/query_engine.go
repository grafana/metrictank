package api

import (
	"github.com/raintank/metrictank/api/models"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/stats"
	"github.com/raintank/metrictank/util"
)

var (
	// metric api.request.render.chosen_archive is the archive chosen for the request.
	// 0 means original data, 1 means first agg level, 2 means 2nd
	reqRenderChosenArchive = stats.NewMeter32("api.request.render.chosen_archive", false)
	// metric api.request.render.series is the number of points that need to be fetched for a /render request.
	reqRenderPointsFetched = stats.NewMeter32("api.request.render.points_fetched", false)
	// metric api.request.render.series is the number of points the request will return.
	reqRenderPointsReturned = stats.NewMeter32("api.request.render.points_returned", false)
)

// alignRequests updates the requests with all details for fetching, making sure all metrics are in the same, optimal interval
// luckily, all metrics still use the same aggSettings, making this a bit simpler
// note: it is assumed that all requests have the same from & to.
// also takes a "now" value which we compare the TTL against
func alignRequests(now uint32, reqs []models.Req, s mdata.AggSettings) ([]models.Req, error) {

	tsRange := (reqs[0].To - reqs[0].From)

	// prefer raw first and foremost.
	// if raw interval doesn't retain data long enough, we must
	// find the highest res rollup archive that retains all the data we need.
	// fallback to lowest res option (which *should* have the longest TTL)

	archive := 0
	ttl := s.RawTTL

	if now-s.RawTTL > reqs[0].From {
		for i, agg := range s.Aggs {
			// skip non-ready option.
			if !agg.Ready {
				continue
			}
			archive = i + 1
			ttl = agg.TTL
			if now-agg.TTL <= reqs[0].From {
				break
			}
		}

	}

	var interval uint32
	var listRawIntervals []uint32 // note: only populated when archive 0
	// the first (raw) uses the LCM of different raw intervals, since that's what needed to fulfill a raw request.
	// in edge cases (poorly configured setups) this might make raw interval > 1st rollup
	if archive == 0 {
		seenRawIntervals := make(map[uint32]struct{})
		for _, req := range reqs {
			if _, ok := seenRawIntervals[req.RawInterval]; !ok {
				listRawIntervals = append(listRawIntervals, req.RawInterval)
				seenRawIntervals[req.RawInterval] = struct{}{}
			}
		}
		interval = util.Lcm(listRawIntervals)
	} else {
		interval = s.Aggs[archive-1].Span
	}

	/* we now just need to update the following properties for each req:
	   archive      int    // 0 means original data, 1 means first agg level, 2 means 2nd, etc.
	   archInterval uint32 // the interval corresponding to the archive we'll fetch
	   outInterval  uint32 // the interval of the output data, after any runtime consolidation
	   aggNum       uint32 // how many points to consolidate together at runtime, after fetching from the archive
	*/

	// only apply runtime consolidation (pre data processing in graphite api) if we have to due to non uniform raw intervals
	runtimeConsolidate := archive == 0 && len(listRawIntervals) > 1
	var pointsFetch uint32
	for i := range reqs {
		req := &reqs[i]
		req.Archive = archive
		req.TTL = ttl
		req.OutInterval = interval

		req.ArchInterval = interval
		req.AggNum = 1
		if runtimeConsolidate {
			req.ArchInterval = req.RawInterval
			req.AggNum = req.OutInterval / req.RawInterval
		}
		pointsFetch += tsRange / req.ArchInterval
	}

	reqRenderPointsFetched.ValueUint32(pointsFetch)
	reqRenderPointsReturned.ValueUint32(uint32(len(reqs)) * tsRange / interval)
	reqRenderChosenArchive.Values(archive, len(reqs))

	return reqs, nil
}
