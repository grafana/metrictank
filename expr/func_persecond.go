package expr

import (
	"fmt"
	"math"

	"github.com/grafana/metrictank/api/models"
	"github.com/raintank/schema"
)

type FuncPerSecond struct {
	in       []GraphiteFunc
	maxValue int64
}

func NewPerSecond() GraphiteFunc {
	return &FuncPerSecond{}
}

func (s *FuncPerSecond) Signature() ([]Arg, []Arg) {
	return []Arg{
			ArgSeriesLists{val: &s.in},
			ArgInt{key: "maxValue", opt: true, validator: []Validator{IntPositive}, val: &s.maxValue},
		}, []Arg{
			ArgSeriesList{},
		}
}

func (s *FuncPerSecond) Context(context Context) Context {
	context.consol = 0
	return context
}

func (s *FuncPerSecond) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series, _, err := consumeFuncs(cache, s.in)
	if err != nil {
		return nil, err
	}
	maxValue := math.NaN()
	if s.maxValue > 0 {
		maxValue = float64(s.maxValue)
	}
	var outputs []models.Series
	for _, serie := range series {
		out := pointSlicePool.Get().([]schema.Point)
		for i, v := range serie.Datapoints {
			out = append(out, schema.Point{Ts: v.Ts})
			if i == 0 || math.IsNaN(v.Val) || math.IsNaN(serie.Datapoints[i-1].Val) {
				out[i].Val = math.NaN()
				continue
			}
			diff := v.Val - serie.Datapoints[i-1].Val
			if diff >= 0 {
				out[i].Val = diff / float64(serie.Interval)
			} else if !math.IsNaN(maxValue) && maxValue >= v.Val {
				out[i].Val = (maxValue + diff + 1) / float64(serie.Interval)
			} else {
				out[i].Val = math.NaN()
			}
		}
		s := models.Series{
			Target:     fmt.Sprintf("perSecond(%s)", serie.Target),
			QueryPatt:  fmt.Sprintf("perSecond(%s)", serie.QueryPatt),
			Tags:       serie.Tags,
			Datapoints: out,
			Interval:   serie.Interval,
		}
		outputs = append(outputs, s)
		cache[Req{}] = append(cache[Req{}], s)
	}
	return outputs, nil
}
