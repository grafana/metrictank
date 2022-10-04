package api

import (
	"fmt"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/archives"
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
		out += fmt.Sprintf("    * group %d:\n", i)
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

// Requests indexed by their ArchivesID
type ReqsByArchives [][]models.Req

func (rba *ReqsByArchives) Add(idx int, r models.Req) {
	for len(*rba) <= idx {
		*rba = append(*rba, nil)
	}
	(*rba)[idx] = append((*rba)[idx], r)
}

// OutInterval returns the outinterval of the ReqsByArchives
// this assumes that all requests have been planned to a consistent out interval, of course.
func (rba ReqsByArchives) OutInterval() uint32 {
	for _, reqs := range rba {
		if len(reqs) != 0 {
			return reqs[0].OutInterval
		}
	}
	return 0
}

func (rba ReqsByArchives) HasData() bool {
	for _, reqs := range rba {
		if len(reqs) != 0 {
			return true
		}
	}
	return false
}

func (rba ReqsByArchives) Len() int {
	var cnt int
	for _, reqs := range rba {
		cnt += len(reqs)
	}
	return cnt
}

// GroupData embodies a set of requests broken down by whether requests are MDP-optimizable, and by archivesID
type GroupData struct {
	mdpyes ReqsByArchives // MDP-optimizable requests
	mdpno  ReqsByArchives // not MDP-optimizable reqs
}

func NewGroupData() GroupData {
	return GroupData{}
}

func (gd GroupData) Len() int {
	return gd.mdpno.Len() + gd.mdpyes.Len()
}

// ReqsPlan holds requests that have been planned, broken down by PNGroup and MDP-optimizability
type ReqsPlan struct {
	pngroups map[models.PNGroup]GroupData
	single   GroupData
	cnt      uint32
	archives archives.Index
}

// NewReqsPlan generates a ReqsPlan based on the provided ReqMap.
func NewReqsPlan(reqs ReqMap) ReqsPlan {
	rp := ReqsPlan{
		pngroups: make(map[models.PNGroup]GroupData),
		single:   NewGroupData(),
		cnt:      reqs.cnt,
		archives: archives.NewIndex(),
	}

	for group, groupReqs := range reqs.pngroups {
		data := NewGroupData()
		for _, req := range groupReqs {
			archivesID := rp.archives.ArchivesID(req.RawInterval, req.SchemaId)
			if req.MaxPoints > 0 {
				data.mdpyes.Add(archivesID, req)
			} else {
				data.mdpno.Add(archivesID, req)
			}
		}
		rp.pngroups[group] = data
	}

	for _, req := range reqs.single {
		archivesID := rp.archives.ArchivesID(req.RawInterval, req.SchemaId)
		if req.MaxPoints > 0 {
			rp.single.mdpyes.Add(archivesID, req)
		} else {
			rp.single.mdpno.Add(archivesID, req)
		}
	}

	return rp
}

// PointsFetch returns how many points this plan will fetch when executed
func (rp ReqsPlan) PointsFetch() uint32 {
	var cnt uint32
	for _, rba := range rp.single.mdpyes {
		for _, req := range rba {
			cnt += req.PointsFetch()
		}
	}
	for _, rba := range rp.single.mdpno {
		for _, req := range rba {
			cnt += req.PointsFetch()
		}
	}
	for _, data := range rp.pngroups {
		for _, rba := range data.mdpyes {
			for _, req := range rba {
				cnt += req.PointsFetch()
			}
		}
		for _, rba := range data.mdpno {
			for _, req := range rba {
				cnt += req.PointsFetch()
			}
		}
	}
	return cnt
}

// Dump provides a human readable string representation of the ReqsPlan
func (rp ReqsPlan) Dump() string {
	out := fmt.Sprintf("ReqsPlan (%d entries):\n", rp.cnt)
	out += "  # Groups:\n"
	for i, data := range rp.pngroups {
		out += fmt.Sprintf("    ## group %d\n", i)
		out += "      ### MDP-yes:\n"
		for archivesID, reqs := range data.mdpyes {
			for _, req := range reqs {
				out += fmt.Sprintf("        [%v] %s\n", rp.archives.Get(archivesID), req.DebugString())
			}
		}
		out += "      ### MDP-no:\n"
		for archivesID, reqs := range data.mdpno {
			for _, req := range reqs {
				out += fmt.Sprintf("        [%v] %s\n", rp.archives.Get(archivesID), req.DebugString())
			}
		}
	}
	out += "  # Single\n"
	out += "   ## MDP-yes:\n"
	for archivesID, reqs := range rp.single.mdpyes {
		for _, req := range reqs {
			out += fmt.Sprintf("    [%v] %s\n", rp.archives.Get(archivesID), req.DebugString())
		}
	}
	out += "    ## MDP-no:\n"
	for archivesID, reqs := range rp.single.mdpno {
		for _, req := range reqs {
			out += fmt.Sprintf("    [%v] %s\n", rp.archives.Get(archivesID), req.DebugString())
		}
	}
	return out
}

// PointsReturn estimates the amount of points that will be returned for this request
// best effort: not aware of summarize(), aggregation functions, runtime normalization. but does account for runtime consolidation
func (rp ReqsPlan) PointsReturn(planMDP uint32) uint32 {
	var cnt uint32
	for _, rba := range rp.single.mdpyes {
		for _, req := range rba {
			cnt += req.PointsReturn(planMDP)
		}
	}
	for _, rba := range rp.single.mdpno {
		for _, req := range rba {
			cnt += req.PointsReturn(planMDP)
		}
	}
	for _, data := range rp.pngroups {
		for _, rba := range data.mdpyes {
			for _, req := range rba {
				cnt += req.PointsReturn(planMDP)
			}
		}
		for _, rba := range data.mdpno {
			for _, req := range rba {
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
