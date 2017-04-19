package expr

import (
	"fmt"
	"math"
	"reflect"

	"github.com/raintank/metrictank/api/models"
	"gopkg.in/raintank/schema.v1"
)

type FuncAvgSeries struct {
}

func NewAvgSeries() Func {
	return FuncAvgSeries{}
}

func (s FuncAvgSeries) Signature() ([]argType, []argType) {
	return []argType{seriesLists}, []argType{series}
}

func (s FuncAvgSeries) Init(args []*expr) error {
	return nil
}

func (s FuncAvgSeries) NeedRange(from, to uint32) (uint32, uint32) {
	return from, to
}

func (s FuncAvgSeries) Exec(cache map[Req][]models.Series, inputs ...interface{}) ([]interface{}, error) {
	var series []models.Series
	for _, input := range inputs {
		seriesList, ok := input.([]models.Series)
		if !ok {
			return nil, ErrBadArgument{reflect.TypeOf([]models.Series{}), reflect.TypeOf(input)}
		}
		series = append(series, seriesList...)

	}
	if len(series) == 1 {
		series[0].Target = fmt.Sprintf("averageSeries(%s)", series[0].QueryPatt)
		return []interface{}{series[0]}, nil
	}
	out := pointSlicePool.Get().([]schema.Point)
	for i := 0; i < len(series[0].Datapoints); i++ {
		num := 0
		sum := float64(0)
		for j := 0; j < len(series); j++ {
			p := series[j].Datapoints[i].Val
			if !math.IsNaN(p) {
				num++
				sum += p
			}
		}
		point := schema.Point{
			Ts: series[0].Datapoints[i].Ts,
		}
		if num == 0 {
			point.Val = math.NaN()
		} else {
			point.Val = sum / float64(num)
		}
		out = append(out, point)
	}
	output := models.Series{
		Target:     fmt.Sprintf("averageSeries(%s)", patternsAsArgs(series)),
		Datapoints: out,
		Interval:   series[0].Interval,
	}
	cache[Req{}] = append(cache[Req{}], output)

	return []interface{}{output}, nil
}
