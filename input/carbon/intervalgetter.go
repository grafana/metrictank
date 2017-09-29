package carbon

import (
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/mdata"
)

//IntervalGetter is anything that can return the interval for the given path
//we don't want the carbon plugin to directly talk to an index because the api
//surface is too big and it would couple too tightly which is annoying in unit tests
type IntervalGetter interface {
	GetInterval(name string) int
}

type IndexIntervalGetter struct {
	idx idx.MetricIndex
}

func NewIndexIntervalGetter(idx idx.MetricIndex) IntervalGetter {
	return IndexIntervalGetter{idx}
}

func (i IndexIntervalGetter) GetInterval(name string) int {
	archives := i.idx.GetPath(1, name)
	for _, a := range archives {
		// since the schema rules can't change at runtime and are the schemas are determined at runtime for new entries, they will be the same
		// for any archive with the given name. so the first one we find is enough.
		return a.Interval
	}
	// if it's the first time we're seeing this series, do the more expensive matching
	// note that the index will also do this matching again first time it sees the metric
	_, schema := mdata.MatchSchema(name, 0)
	return schema.Retentions[0].SecondsPerPoint
}
