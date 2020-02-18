package api

import (
	"fmt"
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
			ok = planHighestResMulti(now, from, to, split.mdpno)
			if !ok {
				return nil, errUnSatisfiable
			}
		}
		if len(split.mdpyes) > 0 {
			ok = planLowestResForMDPMulti(now, from, to, planMDP, split.mdpyes)
			if !ok {
				return nil, errUnSatisfiable
			}
			rp.pngroups[group] = split
		}
	}
	for schemaID, reqs := range rp.single.mdpno {
		if len(reqs) == 0 {
			continue
		}
		ok = planHighestResSingles(now, from, to, uint16(schemaID), reqs)
		if !ok {
			return nil, errUnSatisfiable
		}
	}
	for schemaID, reqs := range rp.single.mdpyes {
		if len(reqs) == 0 {
			continue
		}
		ok = planLowestResForMDPSingles(now, from, to, planMDP, uint16(schemaID), reqs)
		if !ok {
			return nil, errUnSatisfiable
		}
	}

	if mpprSoft > 0 {
		// at this point, MDP-optimizable series have already seen a decent resolution reduction
		// so to meet this constraint, we will try to reduce the resolution of non-MDP-optimizable series
		// in the future we can make this even more aggressive and also try to reduce MDP-optimized series even more
		//
		// Note:
		// A) if metrictank is already handling all, or most of your queries, then we have been able to determine
		//    MDP-optimizability very well. In this case we pretty much know that if we need to enforce this option
		//    it is going to have an adverse effect on your queries, and you should probably not use this option,
		//    or we should even get rid of it.
		// B) If the request came from Graphite, we have to assume it may run GR-functions and quite likely we were
		//    too cautious and categorized many series as non-MDP optimizable whereas in reality they should be,
		//    so in that case this option is a welcome way to reduce the impact of big queries.
		//
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
	for _, reqs := range rp.single.mdpyes {
		if len(reqs) != 0 {
			reqRenderChosenArchive.ValueUint32(uint32(reqs[0].Archive) * uint32(len(reqs)))
		}
	}
	for _, reqs := range rp.single.mdpno {
		if len(reqs) != 0 {
			reqRenderChosenArchive.ValueUint32(uint32(reqs[0].Archive) * uint32(len(reqs)))
		}
	}
	for _, data := range rp.pngroups {
		for _, reqs := range data.mdpyes {
			if len(reqs) != 0 {
				reqRenderChosenArchive.ValueUint32(uint32(reqs[0].Archive) * uint32(len(reqs)))
			}
		}
		for _, reqs := range data.mdpno {
			if len(reqs) != 0 {
				reqRenderChosenArchive.ValueUint32(uint32(reqs[0].Archive) * uint32(len(reqs)))
			}
		}
	}
	reqRenderPointsFetched.ValueUint32(rp.PointsFetch())
	reqRenderPointsReturned.ValueUint32(rp.PointsReturn(planMDP))

	return &rp, nil
}

// planHighestResSingles plans all requests of the given retention to their most precise resolution (which may be different for different retentions)
func planHighestResSingles(now, from, to uint32, schemaID uint16, reqs []models.Req) bool {
	rets := mdata.Schemas.Get(uint16(schemaID)).Retentions.Rets
	minTTL := now - from
	archive, ret, ok := findHighestResRet(rets, from, minTTL)
	if ok {
		for i := range reqs {
			req := &reqs[i]
			req.Plan(archive, ret)
		}
	}
	return ok
}

// planLowestResForMDPSingles plans all requests of the given retention to an interval such that requests still return >=mdp/2 points (interval may be different for different retentions)
func planLowestResForMDPSingles(now, from, to, mdp uint32, schemaID uint16, reqs []models.Req) bool {
	if len(reqs) == 0 {
		return true
	}
	rets := mdata.Schemas.Get(uint16(schemaID)).Retentions.Rets
	var archive int
	var ret conf.Retention
	var ok bool
	for i := len(rets) - 1; i >= 0; i-- {
		// skip non-ready option.
		if rets[i].Ready > from {
			continue
		}
		archive, ret, ok = i, rets[i], true
		(&reqs[0]).Plan(i, rets[i])
		if reqs[0].PointsFetch() >= mdp/2 {
			break
		}
	}
	if !ok {
		return false
	}
	for i := range reqs {
		req := &reqs[i]
		req.Plan(archive, ret)
	}
	return true
}

// planHighestResMulti plans all requests of all retentions to the most precise, common, resolution.
func planHighestResMulti(now, from, to uint32, rbr ReqsByRet) bool {
	minTTL := now - from

	var listIntervals []uint32
	var seenIntervals = make(map[uint32]struct{})

	for schemaID, reqs := range rbr {
		if len(reqs) == 0 {
			continue
		}
		rets := mdata.Schemas.Get(uint16(schemaID)).Retentions.Rets
		archive, ret, ok := findHighestResRet(rets, from, minTTL)
		if !ok {
			return false
		}
		for i := range reqs {
			req := &reqs[i]
			req.Plan(archive, ret)
		}
		if _, exists := seenIntervals[reqs[0].ArchInterval]; !exists {
			listIntervals = append(listIntervals, reqs[0].ArchInterval)
			seenIntervals[reqs[0].ArchInterval] = struct{}{}
		}
	}
	interval := util.Lcm(listIntervals)

	// plan all our requests so that they result in the common output interval.
	for schemaID, reqs := range rbr {
		rets := mdata.Schemas.Get(uint16(schemaID)).Retentions.Rets
		for i := range reqs {
			req := &reqs[i]
			req.AdjustTo(interval, from, rets)
		}
	}

	return true
}

// planLowestResForMDPMulti plans all requests of all retentions to the same common interval such that they still return >=mdp/2 points
// note: we can assume all reqs have the same MDP.
func planLowestResForMDPMulti(now, from, to, mdp uint32, rbr ReqsByRet) bool {
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

	// first, extract the set of valid intervals from each retention
	// if a retention has no valid intervals, we can't satisfy the request
	for schemaID, reqs := range rbr {
		if len(reqs) == 0 {
			continue
		}
		var ok bool
		rets := mdata.Schemas.Get(uint16(schemaID)).Retentions.Rets
		var validIntervals []uint32
		for _, ret := range rets {
			if ret.Ready <= from && ret.MaxRetention() >= int(minTTL) {
				ok = true
				validIntervals = append(validIntervals, uint32(ret.SecondsPerPoint))
			}
		}
		if !ok {
			return false
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
	// for each possible combination of intervals, we compute the LCM interval.
	// if the LCM interval honors maxInterval, we compute the score.
	// the interval with the highest score wins.
	combos := util.AllCombinationsUint32(validIntervalss)
	var maxScore int

	lowestInterval := uint32(math.MaxUint32) // lowest interval we find
	var candidateInterval uint32             // the candidate MDP-optimized interval
	var interval uint32                      // will be set to either of the two above
	for _, combo := range combos {
		candidateInterval = util.Lcm(combo)
		if candidateInterval < lowestInterval {
			lowestInterval = candidateInterval
		}
		if candidateInterval > maxInterval {
			continue
		}
		var score int
		for schemaID, reqs := range rbr {
			if len(reqs) == 0 {
				continue
			}
			rets := mdata.Schemas.Get(uint16(schemaID)).Retentions.Rets
			_, ret, ok := findLowestResForInterval(rets, from, minTTL, candidateInterval)
			if !ok {
				panic("planLowestResForMDPMulti: could not find coarsest retention. should never happen because we made sure our candidate LCM is based on what we have")
			}
			score += len(reqs) * ret.SecondsPerPoint
		}
		if score > maxScore {
			maxScore = score
			interval = candidateInterval
		}
	}
	// if we didn't find a suitable MDP-optimized one, just pick the lowest one we've seen.
	if interval == 0 {
		interval = lowestInterval
	}

	// now we finally found our optimal interval that we want to use.
	// plan all our requests so that they result in the common output interval.
	planToMulti(now, from, to, interval, rbr)

	return true
}

// planToMulti plans all requests of all retentions to the same given interval.
// caller must have assured that the requests support this interval, otherwise we will panic
func planToMulti(now, from, to, interval uint32, rbr ReqsByRet) {
	minTTL := now - from
	for schemaID, reqs := range rbr {
		if len(reqs) == 0 {
			continue
		}
		rets := mdata.Schemas.Get(uint16(schemaID)).Retentions.Rets
		archive, ret, ok := findLowestResForInterval(rets, from, minTTL, interval)
		if !ok {
			panic(fmt.Sprintf("planToMulti: could not findLowestResForInterval for desired interval %d", interval))
		}
		for i := range reqs {
			req := &reqs[i]
			req.Plan(archive, ret)
			if interval != uint32(ret.SecondsPerPoint) {
				req.PlanNormalization(interval)
			}
		}
	}
}

// findHighestResRet finds the most precise (lowest interval) retention that:
// * is ready for long enough to accommodate `from`
// * has a long enough TTL, or otherwise the longest TTL
func findHighestResRet(rets []conf.Retention, from, ttl uint32) (int, conf.Retention, bool) {

	var archive int
	var ret conf.Retention
	var ok bool

	for i, retMaybe := range rets {
		// skip non-ready option.
		if retMaybe.Ready > from {
			continue
		}
		archive, ret, ok = i, retMaybe, true

		if uint32(retMaybe.MaxRetention()) >= ttl {
			break
		}
	}

	return archive, ret, ok
}

//findLowestResForInterval finds the coarsest retention out of the list that matches these criteria:
// * it's ready for long enough to accommodate `from`
// * its retention is long enough to cover the desired `ttl`
// * it has an interval that either:
//   - matches the desired interval exactly.
//   - fits into the desired interval. this will return more data at fetch time and require some normalization
// note that because we iterate in descending order we always return an exact match when possible.
func findLowestResForInterval(rets []conf.Retention, from, ttl, interval uint32) (int, conf.Retention, bool) {
	for i := len(rets) - 1; i >= 0; i-- {
		ret := rets[i]
		if ret.Ready <= from &&
			uint32(ret.MaxRetention()) >= ttl &&
			interval%uint32(ret.SecondsPerPoint) == 0 {
			return i, ret, true
		}
	}
	return 0, conf.Retention{}, false
}
