package models

import (
	"fmt"
	"github.com/raintank/metrictank/cluster"
	"github.com/raintank/metrictank/consolidation"
	"github.com/raintank/metrictank/util"
)

type Req struct {
	// these fields can be set straight away:
	Key          string                     `json:"key"`    // metric key aka metric definition id (orgid.<hash>), often same as target for graphite-metrictank requests
	Target       string                     `json:"target"` // the target we should return either to graphite or as if we're graphite.  this is usually the original input string like consolidateBy(key,'sum') except when it's a pattern than it becomes the concrete value like consolidateBy(foo.b*,'sum') -> consolidateBy(foo.bar,'sum') etc
	From         uint32                     `json:"from"`
	To           uint32                     `json:"to"`
	MaxPoints    uint32                     `json:"maxPoints"`
	RawInterval  uint32                     `json:"rawInterval"` // the interval of the raw metric before any consolidation
	Consolidator consolidation.Consolidator `json:"consolidator"`
	Node         cluster.Node               `json:"-"`

	// these fields need some more coordination and are typically set later
	Archive      int    `json:"archive"`      // 0 means original data, 1 means first agg level, 2 means 2nd, etc.
	ArchInterval uint32 `json:"archInterval"` // the interval corresponding to the archive we'll fetch
	TTL          uint32 `json:"ttl"`          // the ttl of the archive we'll fetch
	OutInterval  uint32 `json:"outInterval"`  // the interval of the output data, after any runtime consolidation
	AggNum       uint32 `json:"aggNum"`       // how many points to consolidate together at runtime, after fetching from the archive
}

func NewReq(key, target string, from, to, maxPoints, rawInterval uint32, consolidator consolidation.Consolidator, node cluster.Node) Req {
	return Req{
		key,
		target,
		from,
		to,
		maxPoints,
		rawInterval,
		consolidator,
		node,
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
	return fmt.Sprintf("%s %d - %d . points <= %d. %s - archive %d, rawInt %d, archInt %d, outInt %d, aggNum %d",
		r.Key, r.From, r.To, r.MaxPoints, r.Consolidator, r.Archive, r.RawInterval, r.ArchInterval, r.OutInterval, r.AggNum)
}
