package expr

import (
	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/errors"
	"github.com/grafana/metrictank/schema"
)

type FuncGroupByNodes struct {
	in         GraphiteFunc
	aggregator string
	nodes      []expr
	byNode     bool
}

func NewGroupByNodesConstructor(groupByNode bool) func() GraphiteFunc {
	return func() GraphiteFunc {
		return &FuncGroupByNodes{byNode: groupByNode}
	}
}

func (s *FuncGroupByNodes) Signature() ([]Arg, []Arg) {
	// This function supports both groupByNode and groupByNodes,
    // but the signatures of them are different in Graphite.
    if !s.byNode {
		return []Arg{
			ArgSeriesList{val: &s.in},
			ArgString{val: &s.aggregator, validator: []Validator{IsAggFunc}},
			ArgStringsOrInts{val: &s.nodes},
		}, []Arg{ArgSeries{}}
	}
    // groupByNode accepts either an integer node num or a string tag key
	s.nodes = append(s.nodes, expr{})
	return []Arg{
		ArgSeriesList{val: &s.in},
		ArgStringOrInt{val: &s.nodes[0]},
		ArgString{val: &s.aggregator, validator: []Validator{IsAggFunc}},
	}, []Arg{ArgSeries{}}
}

func (s *FuncGroupByNodes) Context(context Context) Context {
	context.PNGroup = 0
	return context
}

func (s *FuncGroupByNodes) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	if len(series) == 0 {
		return series, nil
	}

	if len(s.nodes) == 0 {
		return nil, errors.NewBadRequest("No nodes specified")
	}

	type Group struct {
		s []models.Series
		m models.SeriesMeta
	}
	groups := make(map[string]Group)

	// Group series by nodes, this is mostly similar to GroupByTags,
    // except that the group keys are different.
	for _, serie := range series {
		key := aggKey(serie, s.nodes)
		group := groups[key]
		group.s = append(group.s, serie)
		group.m = group.m.Merge(serie.Meta)
		groups[key] = group
	}

	// Similar to FuncGroupByTags, apply aggregate functions to each group
	output := make([]models.Series, 0, len(groups))
	aggFunc := getCrossSeriesAggFunc(s.aggregator)

	for key, group := range groups {
		consolidators, queryConsolidators := summarizeCons(group.s)
		outSeries := models.Series{
			Target:       key,
			QueryPatt:    key,
			Interval:     series[0].Interval,
			Consolidator: consolidators,
			QueryCons:    queryConsolidators,
			QueryFrom:    group.s[0].QueryFrom,
			QueryTo:      group.s[0].QueryTo,
			QueryMDP:     group.s[0].QueryMDP,
			QueryPNGroup: group.s[0].QueryPNGroup,
			Meta:         group.m,
		}
		outSeries.SetTags()
		outSeries.Datapoints = pointSlicePool.Get().([]schema.Point)
		group.s = Normalize(dataMap, group.s)
		aggFunc(group.s, &outSeries.Datapoints)
		dataMap.Add(Req{}, outSeries)
		output = append(output, outSeries)
	}
	return output, nil
}
