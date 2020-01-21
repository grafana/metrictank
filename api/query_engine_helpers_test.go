package api

import (
	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/consolidation"
	"github.com/grafana/metrictank/test"
)

type archive struct {
	i           uint8
	interval    uint32
	ttl         uint32
	outInterval uint32
	aggNum      uint32
}

type reqProp struct {
	raw      uint32
	schemaID uint16
	aggID    uint16
}

func NewReqProp(raw uint32, schemaID, aggID uint16) reqProp {
	return reqProp{raw, schemaID, aggID}
}

func generate(from, to uint32, reqs []reqProp) ([]models.Req, []models.Req) {
	var in []models.Req
	mdp := uint32(0)          // for these tests, always disable MDP optimization
	cons := consolidation.Avg // for these tests, consolidator actually doesn't matter
	for i, r := range reqs {
		in = append(in, reqRaw(test.GetMKey(i), from, to, mdp, r.raw, cons, r.schemaID, r.aggID))
	}

	out := make([]models.Req, len(in))
	copy(out, in)
	return in, out
}

func adjust(r *models.Req, archive uint8, archInterval, outInterval, ttl uint32) {
	r.Archive = archive
	r.ArchInterval = archInterval
	r.TTL = ttl
	r.OutInterval = outInterval
	r.AggNum = outInterval / archInterval
}
