package models

import (
	"fmt"
	"github.com/raintank/metrictank/cluster"
	"github.com/raintank/metrictank/consolidation"
	"github.com/raintank/metrictank/util"
)

type Req struct {
	// these fields can be set straight away:
	Key          string                     `json:"key"`     // metric key aka metric definition id (orgid.<hash>), often same as target for graphite-metrictank requests
	Target       string                     `json:"target"`  // the target we should return either to graphite or as if we're graphite.  simply the graphite metric key from the index
	Pattern      string                     `json:"pattern"` // the original query pattern specified by user (not wrapped by any functions). e.g. `foo.b*`. To be able to tie the result data back to the data need as requested
	From         uint32                     `json:"from"`
	To           uint32                     `json:"to"`
	MaxPoints    uint32                     `json:"maxPoints"`
	RawInterval  uint32                     `json:"rawInterval"`  // the interval of the raw metric before any consolidation
	Consolidator consolidation.Consolidator `json:"consolidator"` // consolidation method for rollup archive and normalization. (not runtime consolidation)
	// requested consolidation method: either same as Consolidator, or 0 (meaning use configured default)
	// we need to make this differentiation to tie back to the original request (and we can't just fill in the concrete consolidation in the request,
	// because one request may result in multiple series with different consolidators)
	ConsReq  consolidation.Consolidator `json:"consolidator_req"`
	Node     cluster.Node               `json:"-"`
	SchemaId uint16                     `json:"schemaId"`
	AggId    uint16                     `json:"aggId"`

	// these fields need some more coordination and are typically set later
	Archive      int    `json:"archive"`      // 0 means original data, 1 means first agg level, 2 means 2nd, etc.
	ArchInterval uint32 `json:"archInterval"` // the interval corresponding to the archive we'll fetch
	TTL          uint32 `json:"ttl"`          // the ttl of the archive we'll fetch
	OutInterval  uint32 `json:"outInterval"`  // the interval of the output data, after any runtime consolidation
	AggNum       uint32 `json:"aggNum"`       // how many points to consolidate together at runtime, after fetching from the archive
}

func NewReq(key, target, patt string, from, to, maxPoints, rawInterval uint32, cons, consReq consolidation.Consolidator, node cluster.Node, schemaId, aggId uint16) Req {
	return Req{
		key,
		target,
		patt,
		from,
		to,
		maxPoints,
		rawInterval,
		cons,
		consReq,
		node,
		schemaId,
		aggId,
		-1, // this is supposed to be updated still!
		0,  // this is supposed to be updated still
		0,  // this is supposed to be updated still
		0,  // this is supposed to be updated still
		0,  // this is supposed to be updated still
	}
}

func (r Req) String() string {
	return fmt.Sprintf("%s %d - %d (%s - %s) span:%ds. points <= %d. %s.", r.Key, r.From, r.To, util.TS(r.From), util.TS(r.To), r.To-r.From-1, r.MaxPoints, r.Consolidator)
}

func (r Req) DebugString() string {
	return fmt.Sprintf("Req key=%q target=%q pattern=%q %d - %d (%s - %s) (span %d) maxPoints=%d rawInt=%d cons=%s consReq=%d schemaId=%d aggId=%d archive=%d archInt=%d ttl=%d outInt=%d aggNum=%d",
		r.Key, r.Target, r.Pattern, r.From, r.To, util.TS(r.From), util.TS(r.To), r.To-r.From-1, r.MaxPoints, r.RawInterval, r.Consolidator, r.ConsReq, r.SchemaId, r.AggId, r.Archive, r.ArchInterval, r.TTL, r.OutInterval, r.AggNum)
}
