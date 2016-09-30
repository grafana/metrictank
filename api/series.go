package api

import (
	"gopkg.in/raintank/schema.v1"
)

//go:generate msgp
type Series struct {
	Target     string // will be set to the target attribute of the given request
	Datapoints []schema.Point
	Interval   uint32
}

type SeriesByTarget []Series

func (g SeriesByTarget) Len() int           { return len(g) }
func (g SeriesByTarget) Swap(i, j int)      { g[i], g[j] = g[j], g[i] }
func (g SeriesByTarget) Less(i, j int) bool { return g[i].Target < g[j].Target }
