package api

import (
	"math"
	"net/http"
	"reflect"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/api/response"
	"github.com/grafana/metrictank/conf"
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
	// metric api.request.render.points_returned is the number of points the request will return
	// best effort: not aware of summarize(), aggregation functions, runtime normalization. but does account for runtime consolidation
	reqRenderPointsReturned = stats.NewMeter32("api.request.render.points_returned", false)

	errUnSatisfiable   = response.NewError(http.StatusNotFound, "request cannot be satisfied due to lack of available retentions")
	errMaxPointsPerReq = response.NewError(http.StatusRequestEntityTooLarge, "request exceeds max-points-per-req-hard limit. Reduce the time range or number of targets or ask your admin to increase the limit.")
)

func getRetentions(req models.Req) []conf.Retention {
	return mdata.Schemas.Get(req.SchemaId).Retentions.Rets
}

// planRequests updates the requests with all details for fetching.
// Notes:
// [1] MDP-optimization may reduce amount of points down to MDP/2, but not lower. TODO: how about reduce to MDP exactly if possible, and a bit lower otherwise
//     Typically MDP matches number of pixels, which is very dense. So MDP/2 is still quite dense, and for our purposes we consider MDP/2 points to contain the same amount of "information".
// [2] MDP-optimizable requests (when considered by themselves) incur no significant information loss. See [1]
//     Though consider this case:
//     series A 10s:7d,5min:70d
//     series B 10s:7d,4min:30d
//     Let's say a request comes in for 2 days worth of data with MDP=800. Using the high-res data would be 17280 points and require runtime consolidation
//     Both series can be MDP optimized: pick archive=1 and get 576 and 720 points respectively. Neither lost information.
//     However, if it then turns out that both series need to be combined in an aggregation function, they need to be reduced to 20 min resolution, which results in coarse points.
//     Thus MDP-optimized series can still possibly result in some information loss, though this seems quite rare.
//     If you want to aggregate different data together, just give it compatible intervals. For our purposes we will consider MDP-optimizing safe.
// [3] Requests in the same PNGroup will need to be normalized together anyway.
//     Because the consolidation function for normalization is always set taking into account the rollups that we have (see executePlan()) we can better read from a coarser archive.
//     Any request in a PNGroup has already been vetted to be worthy of pre-normalization, thus there is absolutely no loss of information.
//
// planRequests follows these steps:
// 1) Initial parameters.
//    select the highest resolution possible within TTL for all requests. there's 4 cases:
//    * requests in the same PNGroup,    and MDP-optimizable: reduce aggressively: to longest common interval such that points >=MDP/2
//    * requests in the same PNGroup but not MDP-optimizable: reduce conservatively: to shortest common interval that still meets TTL
//    * MDP optimizable singles     : longest interval such that points >= MDP/2
//    * non-MDP-optimizable singles : shortest interval that still meets TTL
//
// 2) apply max-points-per-req-soft (meaning: pick coarser data as needed)
//    The optimizations in the previous step should increase the odds of meeting this limit.
//    If we still breach this limit, we could
//    a) reduce the already MDP-optimized ones further but that would definitely result in loss of accuracy
//    b) reduce non-MDP-optimizable series.
//    For "fairness" across series, and because we used to simply reduce any series without regard for how it would be used, we pick the latter. better would be both
// 3) subject to max-points-per-req-hard: reject the query if it can't be met
//
// note: it is assumed that all requests have the same from & to.
// also takes a "now" value which we compare the TTL against
func planRequests(now, from, to uint32, reqs *ReqMap, planMDP uint32, mpprSoft, mpprHard int) (*ReqsPlan, error) {

	ok, rp := false, NewReqsPlan(*reqs)

	for group, split := range rp.pngroups {
		if len(split.mdpno) > 0 {
			split.mdpno, ok = planHighestResMulti(now, from, to, split.mdpno)
			if !ok {
				return nil, errUnSatisfiable
			}
		}
		if len(split.mdpyes) > 0 {
			split.mdpyes, ok = planLowestResForMDPMulti(now, from, to, planMDP, split.mdpyes)
			if !ok {
				return nil, errUnSatisfiable
			}
			rp.pngroups[group] = split
		}
	}
	for i, req := range rp.single.mdpno {
		rp.single.mdpno[i], ok = planHighestResSingle(now, from, to, req)
		if !ok {
			return nil, errUnSatisfiable
		}
	}
	for i, req := range rp.single.mdpyes {
		rp.single.mdpyes[i], ok = planLowestResForMDPSingle(now, from, to, planMDP, req)
		if !ok {
			return nil, errUnSatisfiable
		}
	}

	if mpprSoft > 0 {
		// at this point, all MDP-optimizable series have already been optimized
		// we can try to reduce the resolution of non-MDP-optimizable series
		// if metrictank is already handling all, or most of your queries, then we have been able to determine
		// MDP-optimizability very well. If the request came from Graphite, we have to assume it may run GR-functions.
		// thus in the former case, we pretty much know that this is going to have an adverse effect on your queries,
		// and you should probably not use this option, or we should even get rid of it.
		// in the latter case though, it's quite likely we were too cautious and categorized many series as non-MDP
		// optimizable whereas in reality they should be, so in that case this option is a welcome way to reduce the
		// impact of big queries
		// we could do two approaches: gradually reduce the interval of all series/groups being read, or just aggressively
		// adjust one group at a time. The latter seems simpler, so for now we do just that.
		if rp.PointsFetch() > uint32(mpprSoft) {
			for group, split := range rp.pngroups {
				if len(split.mdpno) > 0 {
					split.mdpno, ok = planLowestResForMDPMulti(now, from, to, planMDP, split.mdpno)
					if !ok {
						return nil, errUnSatisfiable
					}
					rp.pngroups[group] = split
					if rp.PointsFetch() <= uint32(mpprSoft) {
						goto HonoredSoft
					}
				}
			}
			for i, req := range rp.single.mdpno {
				rp.single.mdpno[i], ok = planLowestResForMDPSingle(now, from, to, planMDP, req)
				if !ok {
					return nil, errUnSatisfiable
				}
				// for every 10 requests we adjusted, check if we honor soft now.
				// note that there may be thousands of requests
				if i%10 == 9 {
					if rp.PointsFetch() <= uint32(mpprSoft) {
						goto HonoredSoft
					}
				}
			}
		}
	}
HonoredSoft:

	if mpprHard > 0 && int(rp.PointsFetch()) > mpprHard {
		return nil, errMaxPointsPerReq

	}

	// send out some metrics and we're done!
	for _, r := range rp.single.mdpyes {
		reqRenderChosenArchive.ValueUint32(uint32(r.Archive))
	}
	for _, r := range rp.single.mdpno {
		reqRenderChosenArchive.ValueUint32(uint32(r.Archive))
	}
	for _, split := range rp.pngroups {
		for _, r := range split.mdpyes {
			reqRenderChosenArchive.ValueUint32(uint32(r.Archive))
		}
		for _, r := range split.mdpno {
			reqRenderChosenArchive.ValueUint32(uint32(r.Archive))
		}
	}
	reqRenderPointsFetched.ValueUint32(rp.PointsFetch())
	reqRenderPointsReturned.ValueUint32(rp.PointsReturn(planMDP))

	return &rp, nil
}

func planHighestResSingle(now, from, to uint32, req models.Req) (models.Req, bool) {
	rets := getRetentions(req)
	minTTL := now - from
	var ok bool
	for i, ret := range rets {
		// skip non-ready option.
		if ret.Ready > from {
			continue
		}
		ok = true
		req.Plan(i, ret)

		if req.TTL >= minTTL {
			break
		}
	}
	return req, ok
}

func planLowestResForMDPSingle(now, from, to, mdp uint32, req models.Req) (models.Req, bool) {
	rets := getRetentions(req)
	var ok bool
	for i := len(rets) - 1; i >= 0; i-- {
		// skip non-ready option.
		if rets[i].Ready > from {
			continue
		}
		ok = true
		req.Plan(i, rets[i])
		if req.PointsFetch() >= mdp/2 {
			break
		}
	}
	return req, ok
}
func planHighestResMulti(now, from, to uint32, reqs []models.Req) ([]models.Req, bool) {
	minTTL := now - from

	var listIntervals []uint32
	var seenIntervals = make(map[uint32]struct{})

	for i := range reqs {
		req := &reqs[i]
		var ok bool
		rets := getRetentions(*req)
		for i, ret := range rets {
			// skip non-ready option.
			if ret.Ready > from {
				continue
			}
			ok = true
			req.Plan(i, ret)

			if req.TTL >= minTTL {
				break
			}
		}
		if !ok {
			return nil, false
		}
		if _, exists := seenIntervals[req.ArchInterval]; !exists {
			listIntervals = append(listIntervals, req.ArchInterval)
			seenIntervals[req.ArchInterval] = struct{}{}
		}
	}
	interval := util.Lcm(listIntervals)

	// plan all our requests so that they result in the common output interval.
	for i := range reqs {
		req := &reqs[i]
		req.AdjustTo(interval, from, getRetentions(*req))
	}

	return reqs, true
}

// note: we can assume all reqs have the same MDP.
func planLowestResForMDPMulti(now, from, to, mdp uint32, reqs []models.Req) ([]models.Req, bool) {
	minTTL := now - from

	// if we were to set each req to their coarsest interval that results in >= MDP/2 points,
	// we'd still have to align them to their LCM interval, which may push them in to
	// "too coarse" territory.
	// instead, we pick the coarsest allowable artificial interval...
	maxInterval := (2 * (to - from)) / mdp
	// ...and then we look for the combination of intervals that scores highest.
	// the bigger the interval the better (load less points), adjusted for number of reqs that
	// have that interval. but their combined LCM may not exceed maxInterval.

	var validIntervalss [][]uint32

	// first, find the unique set of retentions we're dealing with.
	retentions := make(map[uint16]struct{})
	for _, req := range reqs {
		retentions[req.SchemaId] = struct{}{}
	}

	// now, extract the set of valid intervals from each retention
	// if a retention has no valid intervals, we can't satisfy the request
	for schemaID := range retentions {
		var ok bool
		rets := mdata.Schemas.Get(schemaID).Retentions.Rets
		var validIntervals []uint32
		for _, ret := range rets {
			if ret.Ready <= from && ret.MaxRetention() >= int(minTTL) {
				ok = true
				validIntervals = append(validIntervals, uint32(ret.SecondsPerPoint))
			}
		}
		if !ok {
			return nil, false
		}
		// add our sequence of valid intervals to the list, unless it's there already
		var found bool
		for _, v := range validIntervalss {
			if reflect.DeepEqual(v, validIntervals) {
				found = true
				break
			}
		}
		if !found {
			validIntervalss = append(validIntervalss, validIntervals)
		}
	}

	// now, we need to pick a combination of intervals from each interval set.
	// for each possibly combination of intervals, we compute the LCM interval.
	// if if the LCM interval honors maxInterval, we compute the score
	// the interval with the highest score wins.
	// note : we can probably make this more performant by iterating over
	// the retentions, instead of over individual requests
	combos := util.AllCombinationsUint32(validIntervalss)
	var maxScore int

	lowestInterval := uint32(math.MaxUint32) // lowest interval we find
	var candidateInterval uint32             // the candidate MDP-optimized interval
	var interval uint32                      // will be set to either of the two above
	for _, combo := range combos {
		candidateInterval = util.Lcm(combo)
		if candidateInterval <= maxInterval {
			var score int
			for _, req := range reqs {
				rets := getRetentions(req)
				// we know that every request must have a ready retention with an interval that fits into the candidate LCM
				// only a matter of finding the best (largest) one
				for i := len(rets) - 1; i >= 0; i-- {
					ret := rets[i]
					if uint32(ret.SecondsPerPoint) <= candidateInterval && candidateInterval%uint32(ret.SecondsPerPoint) == 0 && ret.Ready <= from && req.TTL >= minTTL {
						score += ret.SecondsPerPoint
					}
				}
			}
			if score > maxScore {
				maxScore = score
				interval = candidateInterval
			}
		}
		if candidateInterval < lowestInterval {
			lowestInterval = candidateInterval
		}
	}
	// if we didn't find a suitable MDP-optimized one, just pick the lowest one we've seen.
	if interval == 0 {
		interval = lowestInterval
	}
	// now we finally found our optimal interval that we want to use.
	// plan all our requests so that they result in the common output interval.
	for i := range reqs {
		req := &reqs[i]
		rets := getRetentions(*req)
		for i := len(rets) - 1; i >= 0; i-- {
			ret := rets[i]
			if ret.Ready <= from && req.TTL >= minTTL {
				if uint32(ret.SecondsPerPoint) == interval {
					req.Plan(i, ret)
					break
				}
				if interval%uint32(ret.SecondsPerPoint) == 0 {
					req.Plan(i, ret)
					req.PlanNormalization(interval)
					break
				}
			}
		}

	}
	return reqs, true
}
