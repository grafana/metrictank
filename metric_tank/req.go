package main

import (
	"fmt"
	"github.com/raintank/raintank-metric/metric_tank/consolidation"
)

type Req struct {
	key          string
	from         uint32
	to           uint32
	minPoints    uint32
	maxPoints    uint32
	consolidator consolidation.Consolidator
}

func NewReq(key string, from, to, minPoints, maxPoints uint32, consolidator consolidation.Consolidator) Req {
	return Req{
		key,
		from,
		to,
		minPoints,
		maxPoints,
		consolidator,
	}
}

func (r Req) String() string {
	return fmt.Sprintf("%s %d - %d (%s - %s) span:%ds. %d <= points <= %d. %s", r.key, r.from, r.to, TS(r.from), TS(r.to), r.to-r.from-1, r.minPoints, r.maxPoints, r.consolidator)
}
