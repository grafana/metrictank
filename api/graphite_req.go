package api

import (
	"fmt"

	"github.com/grafana/metrictank/api/models"
)

// ReqMap is a map of requests of data,
// it has single requests for which no pre-normalization effort will be performed, and
// requests that can be pre-normalized together to the same resolution, bundled by their PNGroup
type ReqMap struct {
	single   []models.Req
	pngroups map[models.PNGroup][]models.Req
	cnt      uint32
}

func NewReqMap() *ReqMap {
	return &ReqMap{
		pngroups: make(map[models.PNGroup][]models.Req),
	}
}

// Add adds a models.Req to the ReqMap
func (r *ReqMap) Add(req models.Req) {
	r.cnt++
	if req.PNGroup == 0 {
		r.single = append(r.single, req)
		return
	}
	r.pngroups[req.PNGroup] = append(r.pngroups[req.PNGroup], req)
}

// Dump provides a human readable string representation of the ReqsMap
func (r ReqMap) Dump() string {
	out := fmt.Sprintf("ReqsMap (%d entries):\n", r.cnt)
	out += "  Groups:\n"
	for i, reqs := range r.pngroups {
		out += fmt.Sprintf("    * group %d:", i)
		for _, r := range reqs {
			out += "      " + r.DebugString() + "\n"
		}
	}
	out += "  Single:\n"
	for _, r := range r.single {
		out += "    " + r.DebugString() + "\n"
	}
	return out
}

// PNGroupSplit embodies a PNGroup broken down by whether requests are MDP-optimizable
type PNGroupSplit struct {
	mdpyes []models.Req // MDP-optimizable requests
	mdpno  []models.Req // not MDP-optimizable reqs
}

// ReqsPlan holds requests that have been planned, broken down by PNGroup and MDP-optimizability
type ReqsPlan struct {
	pngroups map[models.PNGroup]PNGroupSplit
	single   PNGroupSplit
	cnt      uint32
}

// NewReqsPlan generates a ReqsPlan based on the provided ReqMap.
func NewReqsPlan(reqs ReqMap) ReqsPlan {
	rp := ReqsPlan{
		pngroups: make(map[models.PNGroup]PNGroupSplit),
		cnt:      reqs.cnt,
	}
	for group, groupReqs := range reqs.pngroups {
		var split PNGroupSplit
		for _, req := range groupReqs {
			if req.MaxPoints > 0 {
				split.mdpyes = append(split.mdpyes, req)
			} else {
				split.mdpno = append(split.mdpno, req)
			}
		}
		rp.pngroups[group] = split
	}
	for _, req := range reqs.single {
		if req.MaxPoints > 0 {
			rp.single.mdpyes = append(rp.single.mdpyes, req)
		} else {
			rp.single.mdpno = append(rp.single.mdpno, req)
		}
	}
	return rp
}

// PointsFetch returns how many points this plan will fetch when executed
func (rp ReqsPlan) PointsFetch() uint32 {
	var cnt uint32
	for _, r := range rp.single.mdpyes {
		cnt += r.PointsFetch()
	}
	for _, r := range rp.single.mdpno {
		cnt += r.PointsFetch()
	}
	for _, split := range rp.pngroups {
		for _, r := range split.mdpyes {
			cnt += r.PointsFetch()
		}
		for _, r := range split.mdpno {
			cnt += r.PointsFetch()
		}
	}
	return cnt
}

// Dump provides a human readable string representation of the ReqsPlan
func (rp ReqsPlan) Dump() string {
	out := fmt.Sprintf("ReqsPlan (%d entries):\n", rp.cnt)
	out += "  Groups:\n"
	for i, split := range rp.pngroups {
		out += fmt.Sprintf("    * group %d\nMDP-yes:\n", i)
		for _, r := range split.mdpyes {
			out += "      " + r.DebugString() + "\n"
		}
		out += "  MDP-no:\n"
		for _, r := range split.mdpno {
			out += "      " + r.DebugString() + "\n"
		}
	}
	out += "  Single MDP-yes:\n"
	for _, r := range rp.single.mdpyes {
		out += "    " + r.DebugString() + "\n"
	}
	out += "  Single MDP-no:\n"
	for _, r := range rp.single.mdpno {
		out += "    " + r.DebugString() + "\n"
	}
	return out
}

// PointsReturn estimates the amount of points that will be returned for this request
// best effort: not aware of summarize(), aggregation functions, runtime normalization. but does account for runtime consolidation
func (rp ReqsPlan) PointsReturn(planMDP uint32) uint32 {
	var cnt uint32
	for _, r := range rp.single.mdpyes {
		cnt += r.PointsReturn(planMDP)
	}
	for _, r := range rp.single.mdpno {
		cnt += r.PointsReturn(planMDP)
	}
	for _, split := range rp.pngroups {
		for _, r := range split.mdpyes {
			cnt += r.PointsReturn(planMDP)
		}
		for _, r := range split.mdpno {
			cnt += r.PointsReturn(planMDP)
		}
	}
	return cnt
}

// List returns the requests contained within the plan as a slice
func (rp ReqsPlan) List() []models.Req {
	l := make([]models.Req, 0, rp.cnt)
	l = append(l, rp.single.mdpno...)
	l = append(l, rp.single.mdpyes...)
	for _, g := range rp.pngroups {
		l = append(l, g.mdpno...)
		l = append(l, g.mdpyes...)
	}
	return l
}
