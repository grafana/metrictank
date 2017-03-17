package carbon

import (
	"github.com/raintank/metrictank/idx"
	"github.com/raintank/metrictank/mdata"
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
	nodes, err := i.idx.Find(1, name, 0)
	if err != nil {
		panic(err)
	}
	for _, n := range nodes {
		// since the schema rules can't change at runtime and are the schemas are determined at runtime for new entries, they will be the same
		// for any def/archive with the given name. so the first one we find is enough.
		if n.Leaf {
			return mdata.Schemas.Get(n.Defs[0].SchemaId).Retentions[0].SecondsPerPoint
		}
	}
	// if it's the first time we're seeing this series, do the more expensive matching
	// note that the index will also do this matching again first time it sees the metric
	_, schema := mdata.MatchSchema(name)
	return schema.Retentions[0].SecondsPerPoint
}
