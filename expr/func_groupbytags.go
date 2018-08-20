package expr

import (
	"bytes"
	"errors"
	"sort"
	"strings"

	"github.com/grafana/metrictank/api/models"
	"github.com/raintank/schema"
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

	if len(s.tags) == 0 {
		return nil, errors.New("No tags specified")
	}

	groups := make(map[string][]models.Series)
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
			thisName := strings.Split(serie.Target, ";")[0]
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
		name := strings.SplitN(serie.Target, ";", 2)[0]

		buffer.Reset()

		if useName {
			buffer.WriteString(name)
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

		groups[key] = append(groups[key], serie)
	}

	output := make([]models.Series, 0, len(groups))
	aggFunc := getCrossSeriesAggFunc(s.aggregator)

	// Now, for each key perform the requested aggregation
	for name, groupSeries := range groups {
		cons, queryCons := summarizeCons(series)
		newSeries := models.Series{
			Target:       name,
			QueryPatt:    name,
			Interval:     series[0].Interval,
			Consolidator: cons,
			QueryCons:    queryCons,
		}
		newSeries.SetTags()

		newSeries.Datapoints = pointSlicePool.Get().([]schema.Point)
		aggFunc(groupSeries, &newSeries.Datapoints)
		cache[Req{}] = append(cache[Req{}], newSeries)

		output = append(output, newSeries)
	}

	return output, nil
}
