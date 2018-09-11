package api

import (
	"math"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/api/response"
	"github.com/grafana/metrictank/consolidation"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/stats"
	"github.com/grafana/metrictank/util"
)

var (
	// metric api.request.render.chosen_archive is the archive chosen for the request.
	// 0 means original data, 1 means first agg level, 2 means 2nd
	reqRenderChosenArchive = stats.NewMeter32("api.request.render.chosen_archive", false)
	// metric api.request.render.points_fetched is the number of points that need to be fetched for a /render request.
	reqRenderPointsFetched = stats.NewMeter32("api.request.render.points_fetched", false)
	// metric api.request.render.points_returned is the number of points the request will return.
	reqRenderPointsReturned = stats.NewMeter32("api.request.render.points_returned", false)

	errUnSatisfiable   = response.NewError(404, "request cannot be satisfied due to lack of available retentions")
	errMaxPointsPerReq = response.NewError(413, "request exceeds max-points-per-req-hard limit. Reduce the time range or number of targets or ask your admin to increase the limit.")
)

// alignRequests updates the requests with all details for fetching, making sure all metrics are in the same, optimal interval
// note: it is assumed that all requests have the same maxDataPoints, from & to.
// also takes a "now" value which we compare the TTL against
func alignRequests(now, from, to uint32, reqs []models.Req) ([]models.Req, uint32, uint32, error) {
	tsRange := to - from

	var listIntervals []uint32
	var seenIntervals = make(map[uint32]struct{})
	var targets = make(map[string]struct{})

	for i := range reqs {
		req := &reqs[i]
		req.Archive = -1
		targets[req.Target] = struct{}{}
	}
	numTargets := uint32(len(targets))
	minTTL := now - reqs[0].From

	minIntervalSoft := uint32(0)
	minIntervalHard := uint32(0)

	if maxPointsPerReqSoft > 0 {
		minIntervalSoft = uint32(math.Ceil(float64(tsRange) / (float64(maxPointsPerReqSoft) / float64(numTargets))))
	}
	if maxPointsPerReqHard > 0 {
		minIntervalHard = uint32(math.Ceil(float64(tsRange) / (float64(maxPointsPerReqHard) / float64(numTargets))))
	}

	// set preliminary settings. may be adjusted further down
	// but for now:
	// for each req, find the highest res archive
	// (starting with raw, then rollups in decreasing precision)
	// that retains all the data we need and does not exceed minIntervalSoft.
	// fallback to lowest res option (which *should* have the longest TTL)
	for i := range reqs {
		req := &reqs[i]
		retentions := mdata.Schemas.Get(req.SchemaId).Retentions
		for i, ret := range retentions {
			// skip non-ready option.
			if !ret.Ready {
				continue
			}
			req.Archive = i
			req.TTL = uint32(ret.MaxRetention())
			if i == 0 {
				// The first retention is raw data, so use its native interval
				req.ArchInterval = req.RawInterval
			} else {
				req.ArchInterval = uint32(ret.SecondsPerPoint)
			}

			if req.TTL >= minTTL && req.ArchInterval >= minIntervalSoft {
				break
			}
		}
		if req.Archive == -1 {
			return nil, 0, 0, errUnSatisfiable
		}

		if _, ok := seenIntervals[req.ArchInterval]; !ok {
			listIntervals = append(listIntervals, req.ArchInterval)
			seenIntervals[req.ArchInterval] = struct{}{}
		}
	}

	// due to different retentions coming into play, different requests may end up with different resolutions
	// we all need to emit them at the same interval, the LCM interval >= interval of the req
	interval := util.Lcm(listIntervals)

	if interval < minIntervalHard {
		return nil, 0, 0, errMaxPointsPerReq
	}

	// now, for all our requests, set all their properties.  we may have to apply runtime consolidation to get the
	// correct output interval if out interval != native.  In that case, we also check whether we can fulfill
	// the request by reading from an archive instead (i.e. whether it has the correct interval.
	// the TTL of lower resolution archives is always assumed to be at least as long so we don't have to check that)

	var pointsFetch uint32
	for i := range reqs {
		req := &reqs[i]
		if req.ArchInterval == interval {
			// the easy case. we can satisfy this req with what we already found
			// just have to set a few more options
			req.OutInterval = req.ArchInterval
			req.AggNum = 1

		} else {
			// the harder case. due to other reqs with different retention settings
			// we have to deliver an interval higher than what we originally came up with

			// let's see first if we can deliver it via lower-res rollup archives, if we have any
			retentions := mdata.Schemas.Get(req.SchemaId).Retentions
			for i, ret := range retentions[req.Archive+1:] {
				archInterval := uint32(ret.SecondsPerPoint)
				if interval == archInterval && ret.Ready {
					// we're in luck. this will be more efficient than runtime consolidation
					req.Archive = req.Archive + 1 + i
					req.ArchInterval = archInterval
					req.TTL = uint32(ret.MaxRetention())
					req.OutInterval = archInterval
					req.AggNum = 1
					break
				}

			}
			if req.ArchInterval != interval {
				// we have not been able to find an archive matching the desired output interval
				// we will have to apply runtime consolidation
				// we use the initially found archive as starting point. there could be some cases - if you have exotic settings -
				// where it may be more efficient to pick a lower res archive as starting point (it would still require an interval
				// divisible by the output interval) but let's not worry about that edge case.
				req.OutInterval = interval
				req.AggNum = interval / req.ArchInterval
			}
		}
		pointsFetch += tsRange / req.ArchInterval
		reqRenderChosenArchive.Value(req.Archive)
	}

	pointsPerSerie := tsRange / interval
	if reqs[0].MaxPoints > 0 && pointsPerSerie > reqs[0].MaxPoints {
		aggNum := consolidation.AggEvery(pointsPerSerie, reqs[0].MaxPoints)
		pointsPerSerie = pointsPerSerie / aggNum
	}

	pointsReturn := uint32(len(reqs)) * pointsPerSerie
	reqRenderPointsFetched.ValueUint32(pointsFetch)
	reqRenderPointsReturned.ValueUint32(pointsReturn)

	return reqs, pointsFetch, pointsReturn, nil
}
