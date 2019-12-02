package api

import (
	"fmt"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/api/response"
	"github.com/grafana/metrictank/conf"
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

// planRequests updates the requests with all details for fetching.
// starting point:
//     if MDP-optimization enabled -> select the coarsest archive that results in pointcount >= MDP/2, which hopefully lets us avoid, or at least minimize runtime consolidation.
//     otherwise, the highest resolution possible within ttl
// then:
// * subjects the request to max-points-per-req-soft settings, lowering resolution to meet the soft setting.
// * subjects to max-points-per-req-hard setting, rejecting queries that can't be met
// * pre-normalization: a clue that series will need to be normalized together, so may as well try to read from archives that are equivalent to normalization
//   need to watch out here, due to MDP-optimization might result in too low res here
//
// note: it is assumed that all requests have the same from & to.
// also takes a "now" value which we compare the TTL against
func planRequests(now, from, to uint32, reqs *ReqMap, planMDP uint32) ([]models.Req, uint32, uint32, error) {

	var singleRets [][]conf.Retention
	for i, req := range reqs.single {
		rets := mdata.Schemas.Get(req.SchemaId).Retentions.Rets
		singleRets = append(singleRets, rets)
		var ok bool
		if req.MaxPoints == 0 {
			req, ok = initialHighestResWithinTTL(now, from, to, req, rets)
		} else {
			req, ok = initialLowestResForMDP(now, from, to, req, rets)
		}
		reqs.single[i] = req
		if !ok {
			return nil, 0, 0, errUnSatisfiable
		}
	}

	for group, groupReqs := range reqs.pngroups {
		fmt.Println("processing group", group)
		var groupRets [][]conf.Retention
		for i, req := range groupReqs {
			rets := mdata.Schemas.Get(req.SchemaId).Retentions.Rets
			groupRets = append(singleRets, rets)
			var ok bool
			if req.MaxPoints == 0 {
				req, ok = initialHighestResWithinTTL(now, from, to, req, rets)
			} else {
				req, ok = initialLowestResForMDP(now, from, to, req, rets)
			}
			reqs.pngroups[group][i] = req
			if !ok {
				return nil, 0, 0, errUnSatisfiable
			}
		}
		reqs, _, _, _ := alignRequests(now, from, to, groupReqs)

	}

	pointsReturn := uint32(len(reqs)) * pointsPerSerie
	reqRenderPointsFetched.ValueUint32(pointsFetch)
	reqRenderPointsReturned.ValueUint32(pointsReturn)

	return reqs, pointsFetch, pointsReturn, nil
}

func initialHighestResWithinTTL(now, from, to uint32, req models.Req, rets []conf.Retention) (models.Req, bool) {
	minTTL := now - from
	var ok bool
	for i, ret := range rets {
		// skip non-ready option.
		if ret.Ready > from {
			continue
		}
		ok = true
		req.Archive = uint8(i)
		req.TTL = uint32(ret.MaxRetention())
		if i == 0 {
			// The first retention is raw data, so use its native interval
			req.ArchInterval = req.RawInterval
		} else {
			req.ArchInterval = uint32(ret.SecondsPerPoint)
		}
		req.OutInterval = req.ArchInterval
		req.AggNum = 1

		if req.TTL >= minTTL {
			break
		}
	}
	return req, ok
}

func initialLowestResForMDP(now, from, to uint32, req models.Req, rets []conf.Retention) (models.Req, bool) {
	tsRange := to - from
	var ok bool
	for i := len(rets) - 1; i >= 0; i-- {
		ret := rets[i]
		// skip non-ready option.
		if ret.Ready > from {
			continue
		}
		ok = true
		req.Archive = uint8(i)
		req.TTL = uint32(ret.MaxRetention())
		if i == 0 {
			// The first retention is raw data, so use its native interval
			req.ArchInterval = req.RawInterval
		} else {
			req.ArchInterval = uint32(ret.SecondsPerPoint)
		}
		req.OutInterval = req.ArchInterval
		req.AggNum = 1

		numPoints := tsRange / req.ArchInterval
		if numPoints >= req.MaxPoints/2 {
			// if numPoints > req.MaxPoints, we may be able to set up normalization here
			// however, lets leave non-normalized for now. maybe this function will be used for PNGroups later.
			// we wouldn't want to set up AggNum=2 if we need AggNum=3 for the LCM of the PNGroup
			// why? imagine this scenario:
			// interval 10s, numPoints 1000, req.MaxPoints 800 -> if we set AggNum to 2 now, we change interval to 20s
			// but if we have a PNGroup with another series that has interval 30s, we would bring everything to 60s, needlessly coarse
			// (we should bring everything to 30s instead)
			// TODO AggNum/OutInterval not correct here
			break
		}
	}
	return req, ok
}
func alignRequests(now, from, to uint32, reqs []models.Req) ([]models.Req, uint32, uint32, error) {

	var listIntervals []uint32
	var seenIntervals = make(map[uint32]struct{})
	var targets = make(map[string]struct{})

	for _, req := range reqs {
		targets[req.Target] = struct{}{}
	}

	// due to different retentions coming into play, different requests may end up with different resolutions
	// we all need to emit them at the same interval, the LCM interval >= interval of the req
	interval := util.Lcm(listIntervals)

	// ??? NOW WHAT TODO RESUME HERE :) :{)

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
			retentions := mdata.Schemas.Get(req.SchemaId).Retentions.Rets
			for i, ret := range retentions[req.Archive+1:] {
				archInterval := uint32(ret.SecondsPerPoint)
				if interval == archInterval && ret.Ready <= from {
					// we're in luck. this will be more efficient than runtime consolidation
					req.Archive = req.Archive + 1 + uint8(i)
					req.ArchInterval = archInterval
					req.TTL = uint32(ret.MaxRetention())
					req.OutInterval = archInterval
					req.AggNum = 1
					break
				}

			}
			if req.ArchInterval != interval {
				// we have not been able to find an archive matching the desired output interval
				// we will have to apply normalization
				// we use the initially found archive as starting point. there could be some cases - if you have exotic settings -
				// where it may be more efficient to pick a lower res archive as starting point (it would still require an interval
				// divisible by the output interval) but let's not worry about that edge case.
				req.OutInterval = interval
				req.AggNum = interval / req.ArchInterval
			}
		}
		pointsFetch += tsRange / req.ArchInterval
		reqRenderChosenArchive.ValueUint32(uint32(req.Archive))
	}

	pointsPerSerie := tsRange / interval

	// TODO series are not same resolution, need to account for separate intervals
	if planMDP > 0 && pointsPerSerie > planMDP {
		// note that we don't assign to req.AggNum here, because that's only for normalization.
		// MDP runtime consolidation doesn't look at req.AggNum
		aggNum := consolidation.AggEvery(pointsPerSerie, reqs[0].MaxPoints)
		pointsPerSerie /= aggNum
	}

}
