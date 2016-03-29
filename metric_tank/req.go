package main

import (
	"fmt"
	"github.com/raintank/raintank-metric/metric_tank/consolidation"
)

type Req struct {
	// these fields can be set straight away:
	key          string // metric key, often same as target
	target       string // original input string like consolidateBy(key,'sum'). used in output so that graphite doesn't get confused
	from         uint32
	to           uint32
	consolidator consolidation.Consolidator
	maxPoints    uint16

	// these fields need some more coordination and are typically set later
	archive      uint8  // 0 means original data, 1 means first agg level, 2 means 2nd, etc.
	rawInterval  uint16 // the interval of the raw metric before any consolidation
	archInterval uint16 // the interval corresponding to the archive we'll fetch
	outInterval  uint16 // the interval of the output data, after any runtime consolidation
	aggNum       uint32 // how many points to consolidate together at runtime, after fetching from the archive
}

func NewReq(key, target string, from, to uint32, maxPoints uint16, consolidator consolidation.Consolidator) Req {
	return Req{
		key,
		target,
		from,
		to,
		consolidator,
		maxPoints,
		255, // this is supposed to be updated still!
		0,   // this is supposed to be updated still
		0,   // this is supposed to be updated still
		0,   // this is supposed to be updated still
		0,   // this is supposed to be updated still
	}
}

func (r Req) String() string {
	return fmt.Sprintf("%s %d - %d (%s - %s) span:%ds. points <= %d. %s", r.key, r.from, r.to, TS(r.from), TS(r.to), r.to-r.from-1, r.maxPoints, r.consolidator)
}

func (r Req) DebugString() string {
	return fmt.Sprintf("%s %d - %d . points <= %d. %s - archive %d, rawInt %d, archInt %d, outInt %d, aggNum %d",
		r.key, r.from, r.to, r.maxPoints, r.consolidator, r.archive, r.rawInterval, r.archInterval, r.outInterval, r.aggNum)
}
