package api

import (
	"fmt"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/mdata"
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

// Requests indexed by their retention ID
type ReqsByRet [][]models.Req

// OutInterval returns the outinterval of the ReqsByRet
// this assumes that all requests have been planned to a consistent out interval, of course.
func (rbr ReqsByRet) OutInterval() uint32 {
	for _, reqs := range rbr {
		if len(reqs) != 0 {
			return reqs[0].OutInterval
		}
	}
	return 0
}

func (rbr ReqsByRet) HasData() bool {
	for _, reqs := range rbr {
		if len(reqs) != 0 {
			return true
		}
	}
	return false
}

// GroupData embodies a PNGroup broken down by whether requests are MDP-optimizable, and by retention
type GroupData struct {
	mdpyes ReqsByRet // MDP-optimizable requests
	mdpno  ReqsByRet // not MDP-optimizable reqs
}

func NewGroupData() GroupData {
	return GroupData{
		mdpyes: make([][]models.Req, mdata.Schemas.Len()),
		mdpno:  make([][]models.Req, mdata.Schemas.Len()),
	}
}

// ReqsPlan holds requests that have been planned, broken down by PNGroup and MDP-optimizability
type ReqsPlan struct {
	pngroups map[models.PNGroup]GroupData
	single   GroupData
	cnt      uint32
}

// NewReqsPlan generates a ReqsPlan based on the provided ReqMap.
func NewReqsPlan(reqs ReqMap) ReqsPlan {
	rp := ReqsPlan{
		pngroups: make(map[models.PNGroup]GroupData),
		single:   NewGroupData(),
		cnt:      reqs.cnt,
	}
	for group, groupReqs := range reqs.pngroups {
		data := NewGroupData()
		for _, req := range groupReqs {
			if req.MaxPoints > 0 {
				data.mdpyes[req.SchemaId] = append(data.mdpyes[req.SchemaId], req)
			} else {
				data.mdpno[req.SchemaId] = append(data.mdpno[req.SchemaId], req)
			}
		}
		rp.pngroups[group] = data
	}
	for _, req := range reqs.single {
		if req.MaxPoints > 0 {
			rp.single.mdpyes[req.SchemaId] = append(rp.single.mdpyes[req.SchemaId], req)
		} else {
			rp.single.mdpno[req.SchemaId] = append(rp.single.mdpno[req.SchemaId], req)
		}
	}
	return rp
}

// PointsFetch returns how many points this plan will fetch when executed
func (rp ReqsPlan) PointsFetch() uint32 {
	var cnt uint32
	for _, rbr := range rp.single.mdpyes {
		for _, req := range rbr {
			cnt += req.PointsFetch()
		}
	}
	for _, rbr := range rp.single.mdpno {
		for _, req := range rbr {
			cnt += req.PointsFetch()
		}
	}
	for _, data := range rp.pngroups {
		for _, rbr := range data.mdpyes {
			for _, req := range rbr {
				cnt += req.PointsFetch()
			}
		}
		for _, rbr := range data.mdpno {
			for _, req := range rbr {
				cnt += req.PointsFetch()
			}
		}
	}
	return cnt
}

// Dump provides a human readable string representation of the ReqsPlan
func (rp ReqsPlan) Dump() string {
	out := fmt.Sprintf("ReqsPlan (%d entries):\n", rp.cnt)
	out += "  Groups:\n"
	for i, data := range rp.pngroups {
		out += fmt.Sprintf("    * group %d\nMDP-yes:\n", i)
		for schemaID, reqs := range data.mdpyes {
			for _, req := range reqs {
				out += fmt.Sprintf("      [%d] %s\n", schemaID, req.DebugString())
			}
		}
		out += "  MDP-no:\n"
		for schemaID, reqs := range data.mdpno {
			for _, req := range reqs {
				out += fmt.Sprintf("      [%d] %s\n", schemaID, req.DebugString())
			}
		}
	}
	out += "  Single MDP-yes:\n"
	for schemaID, reqs := range rp.single.mdpyes {
		for _, req := range reqs {
			out += fmt.Sprintf("    [%d] %s\n", schemaID, req.DebugString())
		}
	}
	out += "  Single MDP-no:\n"
	for schemaID, reqs := range rp.single.mdpno {
		for _, req := range reqs {
			out += fmt.Sprintf("    [%d] %s\n", schemaID, req.DebugString())
		}
	}
	return out
}

// PointsReturn estimates the amount of points that will be returned for this request
// best effort: not aware of summarize(), aggregation functions, runtime normalization. but does account for runtime consolidation
func (rp ReqsPlan) PointsReturn(planMDP uint32) uint32 {
	var cnt uint32
	for _, rbr := range rp.single.mdpyes {
		for _, req := range rbr {
			cnt += req.PointsReturn(planMDP)
		}
	}
	for _, rbr := range rp.single.mdpno {
		for _, req := range rbr {
			cnt += req.PointsReturn(planMDP)
		}
	}
	for _, data := range rp.pngroups {
		for _, rbr := range data.mdpyes {
			for _, req := range rbr {
				cnt += req.PointsReturn(planMDP)
			}
		}
		for _, rbr := range data.mdpno {
			for _, req := range rbr {
				cnt += req.PointsReturn(planMDP)
			}
		}
	}
	return cnt
}

// List returns the requests contained within the plan as a slice
func (rp ReqsPlan) List() []models.Req {
	l := make([]models.Req, 0, rp.cnt)
	for _, reqs := range rp.single.mdpno {
		l = append(l, reqs...)
	}
	for _, reqs := range rp.single.mdpyes {
		l = append(l, reqs...)
	}
	for _, data := range rp.pngroups {
		for _, reqs := range data.mdpno {
			l = append(l, reqs...)
		}
		for _, reqs := range data.mdpyes {
			l = append(l, reqs...)
		}
	}
	return l
}
