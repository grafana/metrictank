package models

import (
	"fmt"

	"github.com/grafana/metrictank/archives"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/schema"

	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/consolidation"
	"github.com/grafana/metrictank/util"
	opentracing "github.com/opentracing/opentracing-go"
	"github.com/opentracing/opentracing-go/log"
)

//go:generate msgp
//msgp:ignore Req

// Req is a request for data by MKey and parameters such as consolidator, max points, etc
type Req struct {
	// these fields can be set straight away:
	MKey        schema.MKey `json:"key"`     // metric key aka metric definition id (orgid.<hash>), often same as target for graphite-metrictank requests
	Target      string      `json:"target"`  // the target we should return either to graphite or as if we're graphite.  simply the graphite metric key from the index
	Pattern     string      `json:"pattern"` // the original query pattern specified by user (not wrapped by any functions). e.g. `foo.b*`. To be able to tie the result data back to the data need as requested
	From        uint32      `json:"from"`
	To          uint32      `json:"to"`
	MaxPoints   uint32      `json:"maxPoints"`
	PNGroup     PNGroup     `json:"pngroup"`
	RawInterval uint32      `json:"rawInterval"` // the interval of the raw metric before any consolidation
	// the consolidation method for rollup archive and normalization (pre-normalization and runtime normalization). (but not runtime consolidation)
	// ConsReq 0 -> configured value
	// Conseq != 0 -> closest value we can offer based on config
	Consolidator consolidation.Consolidator `json:"consolidator"`
	// requested consolidation method via consolidateBy(), if any.
	// could be 0 if it was not specified or reset by a "special" functions. in which case, we use the configured default.
	// we need to make this differentiation to tie back to the original request (and we can't just fill in the concrete consolidation in the request,
	// because one request may result in multiple series with different consolidators)
	ConsReq  consolidation.Consolidator `json:"consolidator_req"`
	Node     cluster.Node               `json:"-"`
	SchemaId uint16                     `json:"schemaId"`
	AggId    uint16                     `json:"aggId"`

	// these fields need some more coordination and are typically set later (after request planning)
	Archive      uint8  `json:"archive"`      // 0 means original data, 1 means first agg level, 2 means 2nd, etc.
	ArchInterval uint32 `json:"archInterval"` // the interval corresponding to the archive we'll fetch
	TTL          uint32 `json:"ttl"`          // the ttl of the archive we'll fetch
	OutInterval  uint32 `json:"outInterval"`  // the interval of the output data, after any runtime consolidation
	AggNum       uint32 `json:"aggNum"`       // how many points to consolidate together at runtime, after fetching from the archive (normalization)
}

// PNGroup is an identifier for a pre-normalization group: data that can be pre-normalized together
type PNGroup uint64

// NewReq creates a new request. It sets all properties except the ones that need request planning
func NewReq(key schema.MKey, target, patt string, from, to, maxPoints, rawInterval uint32, pngroup PNGroup, cons, consReq consolidation.Consolidator, node cluster.Node, schemaId, aggId uint16) Req {
	return Req{
		MKey:         key,
		Target:       target,
		Pattern:      patt,
		From:         from,
		To:           to,
		MaxPoints:    maxPoints,
		PNGroup:      pngroup,
		RawInterval:  rawInterval,
		Consolidator: cons,
		ConsReq:      consReq,
		Node:         node,
		SchemaId:     schemaId,
		AggId:        aggId,
	}
}

// Init initializes a request based on the metadata that we know of.
// It sets all properties except the ones that need request planning
func (r *Req) Init(archive idx.Archive, cons consolidation.Consolidator, node cluster.Node) {
	r.MKey = archive.Id
	r.Target = archive.NameWithTags()
	r.RawInterval = uint32(archive.Interval)
	r.Consolidator = cons
	r.Node = node
	r.SchemaId = archive.SchemaId
	r.AggId = archive.AggId
}

// Plan updates the planning parameters to match the i'th archive in its retention rules
func (r *Req) Plan(i int, a archives.Archive) {
	r.Archive = uint8(i)
	r.ArchInterval = a.Interval
	r.TTL = a.TTL
	r.OutInterval = r.ArchInterval
	r.AggNum = 1
}

// PlanNormalization updates the planning parameters to accommodate normalization to the specified interval
func (r *Req) PlanNormalization(interval uint32) {
	r.OutInterval = interval
	r.AggNum = interval / r.ArchInterval
}

// AdjustTo adjusts the request to accommodate the requested interval
// notes:
// * the Req MUST have been Plan()'d already!
// * interval MUST be a multiple of the ArchInterval (so we can normalize if needed)
// * the TTL of lower resolution archives is always assumed to be at least as long as the current archive
func (r *Req) AdjustTo(interval, from uint32, as archives.Archives) {

	// if we satisfy the interval with our current settings, nothing left to do
	if r.ArchInterval == interval {
		return
	}

	// let's see if we can deliver it via a lower-res rollup archive.
	for i, a := range as[r.Archive+1:] {
		if interval == a.Interval && a.Ready <= from {
			// we're in luck. this will be more efficient than runtime consolidation
			r.Plan(int(r.Archive)+1+i, a)
			return
		}
	}

	// we will have to apply normalization
	// we use the initially found archive as starting point. there could be some cases - if you have exotic settings -
	// where it may be more efficient to pick a lower res archive as starting point (it would still require an interval
	// that is a factor of the output interval) but let's not worry about that edge case.
	r.PlanNormalization(interval)
}

// PointsFetch returns how many points this request will fetch when executed
func (r Req) PointsFetch() uint32 {
	return (r.To - r.From) / r.ArchInterval
}

// PointsReturn estimates the amount of points that will be returned for this request
// best effort: not aware of summarize(), runtime normalization. but does account for runtime consolidation
func (r Req) PointsReturn(planMDP uint32) uint32 {
	points := (r.To - r.From) / r.OutInterval
	if planMDP > 0 && points > planMDP {
		// note that we don't assign to req.AggNum here, because that's only for normalization.
		// MDP runtime consolidation doesn't look at req.AggNum
		aggNum := consolidation.AggEvery(points, planMDP)
		points /= aggNum
	}
	return points
}

func (r Req) String() string {
	return fmt.Sprintf("%s %d - %d (%s - %s) span:%ds. points <= %d. %s.", r.MKey.String(), r.From, r.To, util.TS(r.From), util.TS(r.To), r.To-r.From-1, r.MaxPoints, r.Consolidator)
}

func (r Req) DebugString() string {
	return fmt.Sprintf("Req key=%q target=%q pattern=%q %d - %d (%s - %s) (span %d) maxPoints=%d pngroup=%d rawInt=%d cons=%s consReq=%d schemaId=%d aggId=%d archive=%d archInt=%d ttl=%d outInt=%d aggNum=%d",
		r.MKey, r.Target, r.Pattern, r.From, r.To, util.TS(r.From), util.TS(r.To), r.To-r.From-1, r.MaxPoints, r.PNGroup, r.RawInterval, r.Consolidator, r.ConsReq, r.SchemaId, r.AggId, r.Archive, r.ArchInterval, r.TTL, r.OutInterval, r.AggNum)
}

// TraceLog puts all request properties in a span log entry
// good for when a span deals with multiple requests
// note that the amount of data generated here can be up to
// 1000~1500 bytes
func (r Req) TraceLog(span opentracing.Span) {
	span.LogFields(
		log.Object("key", r.MKey),
		log.String("target", r.Target),
		log.String("pattern", r.Pattern),
		log.Uint32("from", r.From),
		log.Uint32("to", r.To),
		log.Uint32("span", r.To-r.From-1),
		log.Uint32("mdp", r.MaxPoints),
		log.Uint64("pngroup", uint64(r.PNGroup)),
		log.Uint32("rawInterval", r.RawInterval),
		log.String("cons", r.Consolidator.String()),
		log.String("consReq", r.ConsReq.String()),
		log.Uint32("schemaId", uint32(r.SchemaId)),
		log.Uint32("aggId", uint32(r.AggId)),
		log.Uint32("archive", uint32(r.Archive)),
		log.Uint32("archInterval", r.ArchInterval),
		log.Uint32("TTL", r.TTL),
		log.Uint32("outInterval", r.OutInterval),
		log.Uint32("aggNum", r.AggNum),
	)
}

// Equals compares all fields of a to b for equality.
// Except the Node field: we just compare the node.Name
// rather then doing a deep comparison.
func (a Req) Equals(b Req) bool {
	if a.MKey != b.MKey {
		return false
	}
	if a.Target != b.Target {
		return false
	}
	if a.Pattern != b.Pattern {
		return false
	}
	if a.From != b.From {
		return false
	}
	if a.To != b.To {
		return false
	}
	if a.MaxPoints != b.MaxPoints {
		return false
	}
	if a.PNGroup != b.PNGroup {
		return false
	}
	if a.RawInterval != b.RawInterval {
		return false
	}
	if a.Consolidator != b.Consolidator {
		return false
	}
	if a.ConsReq != b.ConsReq {
		return false
	}
	if a.Node.GetName() != b.Node.GetName() {
		return false
	}
	if a.SchemaId != b.SchemaId {
		return false
	}
	if a.AggId != b.AggId {
		return false
	}
	if a.Archive != b.Archive {
		return false
	}
	if a.ArchInterval != b.ArchInterval {
		return false
	}
	if a.OutInterval != b.OutInterval {
		return false
	}
	if a.TTL != b.TTL {
		return false
	}
	if a.AggNum != b.AggNum {
		return false
	}
	return true
}
