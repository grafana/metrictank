package expr

import (
	"fmt"
	"strings"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
)

type FuncCountSeries struct {
	in []GraphiteFunc
}

func NewCountSeries() GraphiteFunc {
	return &FuncCountSeries{}
}

func (s *FuncCountSeries) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesLists{val: &s.in},
	}, []Arg{ArgSeries{}}
}

func (s *FuncCountSeries) Context(context Context) Context {
	return context
}

func (s *FuncCountSeries) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series, queryPatts, err := consumeFuncs(cache, s.in)
	if err != nil {
		return nil, err
	}

	if len(series) == 0 {
		return series, nil
	}

	cons, queryCons := summarizeCons(series)
	name := fmt.Sprintf("countSeries(%s)", strings.Join(queryPatts, ","))
	out := pointSlicePool.Get().([]schema.Point)

	for _, p := range series[0].Datapoints {
		p.Val = float64(len(series))
		out = append(out, p)
	}

	var meta models.SeriesMeta
	for _, s := range series {
		meta = meta.Merge(s.Meta)
	}

	output := series[0]
	output.Target = name
	output.QueryPatt = name
	output.Tags = map[string]string{"name": name}
	output.Datapoints = out
	output.Consolidator = cons
	output.QueryCons = queryCons
	output.Meta = meta
	cache[Req{}] = append(cache[Req{}], output)

	return []models.Series{output}, nil
}
