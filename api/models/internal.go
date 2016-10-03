package models

import (
	"fmt"
	"github.com/raintank/metrictank/consolidation"
	"github.com/raintank/metrictank/util"
)

type Req struct {
	// these fields can be set straight away:
	Key          string // metric key aka metric definition id (orgid.<hash>), often same as target for graphite-metrictank requests
	Target       string // the target we should return either to graphite or as if we're graphite.  this is usually the original input string like consolidateBy(key,'sum') except when it's a pattern than it becomes the concrete value like consolidateBy(foo.b*,'sum') -> consolidateBy(foo.bar,'sum') etc
	Loc          string // which instance to request from. tcp addr or 'local'
	From         uint32
	To           uint32
	MaxPoints    uint32
	RawInterval  uint32 // the interval of the raw metric before any consolidation
	Consolidator consolidation.Consolidator

	// these fields need some more coordination and are typically set later
	Archive      int    // 0 means original data, 1 means first agg level, 2 means 2nd, etc.
	ArchInterval uint32 // the interval corresponding to the archive we'll fetch
	OutInterval  uint32 // the interval of the output data, after any runtime consolidation
	AggNum       uint32 // how many points to consolidate together at runtime, after fetching from the archive
}

func NewReq(key, target, loc string, from, to, maxPoints, rawInterval uint32, consolidator consolidation.Consolidator) Req {
	return Req{
		key,
		target,
		loc,
		from,
		to,
		maxPoints,
		rawInterval,
		consolidator,
		-1, // this is supposed to be updated still!
		0,  // this is supposed to be updated still
		0,  // this is supposed to be updated still
		0,  // this is supposed to be updated still
	}
}

func (r Req) String() string {
	return fmt.Sprintf("%s %d - %d (%s - %s) span:%ds. points <= %d. %s. loc:%s", r.Key, r.From, r.To, util.TS(r.From), util.TS(r.To), r.To-r.From-1, r.MaxPoints, r.Consolidator, r.Loc)
}

func (r Req) DebugString() string {
	return fmt.Sprintf("%s %d - %d . points <= %d. %s - archive %d, rawInt %d, archInt %d, outInt %d, aggNum %d",
		r.Key, r.From, r.To, r.MaxPoints, r.Consolidator, r.Archive, r.RawInterval, r.ArchInterval, r.OutInterval, r.AggNum)
}

type IndexList struct {
	OrgId int `json:"orgId" form:"orgId" binding:"required"`
}

type IndexGet struct {
	Id string `json:"id" form:"id" binding:"required"`
}

type IndexFind struct {
	Pattern string `json:"pattern" form:"pattern" binding:"required"`
	OrgId   int    `json:"orgId" form:"orgId" binding:"required"`
	From    int64  `json:"from" form:"from"`
}
