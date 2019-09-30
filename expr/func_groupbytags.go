package expr

import (
	"bytes"
	"errors"
	"sort"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
)

type FuncGroupByTags struct {
	in         GraphiteFunc
	aggregator string
	tags       []string
}

func NewGroupByTags() GraphiteFunc {
	return &FuncGroupByTags{}
}

func (s *FuncGroupByTags) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesList{val: &s.in},
		ArgString{val: &s.aggregator, validator: []Validator{IsAggFunc}},
		ArgStrings{val: &s.tags},
	}, []Arg{ArgSeries{}}
}

func (s *FuncGroupByTags) Context(context Context) Context {
	return context
}

func (s *FuncGroupByTags) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series, err := s.in.Exec(cache)
	if err != nil {
		return nil, err
	}

	if len(series) == 0 {
		return series, nil
	}

	if len(s.tags) == 0 {
		return nil, errors.New("No tags specified")
	}

	type Group struct {
		s []models.Series
		m models.SeriesMeta
	}
	groups := make(map[string]Group)
	useName := false

	groupTags := s.tags
	for i, tag := range groupTags {
		if tag == "name" {
			// We handle name explicitly, remove it from tags
			useName = true
			groupTags = append(groupTags[:i], groupTags[i+1:]...)
			break
		}
	}

	nameReplace := ""
	if !useName {
		// if all series have the same name, name becomes one of our tags
		for _, serie := range series {
			thisName := serie.Tags["name"]
			if nameReplace == "" {
				nameReplace = thisName
			} else if nameReplace != thisName {
				nameReplace = s.aggregator
				break
			}
		}
	}

	// Tags need to be sorted
	sort.Strings(groupTags)

	// First pass - group our series together by key
	var buffer bytes.Buffer
	for _, serie := range series {
		buffer.Reset()

		if useName {
			buffer.WriteString(serie.Tags["name"])
		} else {
			buffer.WriteString(nameReplace)
		}

		for _, goal := range groupTags {
			buffer.WriteRune(';')
			buffer.WriteString(goal)
			buffer.WriteRune('=')

			tagVal, ok := serie.Tags[goal]
			if ok {
				buffer.WriteString(tagVal)
			}
		}

		key := buffer.String()

		group := groups[key]
		group.s = append(group.s, serie)
		group.m = group.m.Merge(serie.Meta)
		groups[key] = group
	}

	output := make([]models.Series, 0, len(groups))
	aggFunc := getCrossSeriesAggFunc(s.aggregator)

	// Now, for each key perform the requested aggregation
	for name, group := range groups {
		cons, queryCons := summarizeCons(group.s)

		newSeries := models.Series{
			Target:       name,
			QueryPatt:    name,
			Interval:     series[0].Interval,
			Consolidator: cons,
			QueryCons:    queryCons,
			Meta:         group.m,
		}
		newSeries.SetTags()

		newSeries.Datapoints = pointSlicePool.Get().([]schema.Point)

		aggFunc(group.s, &newSeries.Datapoints)
		cache[Req{}] = append(cache[Req{}], newSeries)

		output = append(output, newSeries)
	}

	return output, nil
}
