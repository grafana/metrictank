package expr

import (
	"fmt"
	"math"
	"reflect"

	"github.com/raintank/metrictank/api/models"
	"gopkg.in/raintank/schema.v1"
)

type FuncSumSeries struct {
}

func NewSumSeries() Func {
	return FuncSumSeries{}
}

func (s FuncSumSeries) Signature() ([]argType, []optArg, []argType) {
	return []argType{seriesLists}, nil, []argType{series}
}

func (s FuncSumSeries) Init(args []*expr, namedArgs map[string]*expr) error {
	return nil
}

func (s FuncSumSeries) NeedRange(from, to uint32) (uint32, uint32) {
	return from, to
}

func (s FuncSumSeries) Exec(cache map[Req][]models.Series, named map[string]interface{}, inputs ...interface{}) ([]interface{}, error) {
	var series []models.Series
	for _, input := range inputs {
		seriesList, ok := input.([]models.Series)
		if !ok {
			return nil, ErrBadArgument{reflect.TypeOf([]models.Series{}), reflect.TypeOf(input)}
		}
		series = append(series, seriesList...)

	}
	if len(series) == 1 {
		series[0].Target = fmt.Sprintf("sumSeries(%s)", series[0].QueryPatt)
		return []interface{}{series[0]}, nil
	}
	out := pointSlicePool.Get().([]schema.Point)
	for i := 0; i < len(series[0].Datapoints); i++ {
		nan := true
		point := schema.Point{
			Ts:  series[0].Datapoints[i].Ts,
			Val: 0,
		}
		for j := 0; j < len(series); j++ {
			if !math.IsNaN(series[j].Datapoints[i].Val) {
				point.Val += series[j].Datapoints[i].Val
				nan = false
			}
		}
		if nan {
			point.Val = math.NaN()
		}
		out = append(out, point)
	}
	output := models.Series{
		Target:     fmt.Sprintf("sumSeries(%s)", patternsAsArgs(series)),
		Datapoints: out,
		Interval:   series[0].Interval,
	}
	cache[Req{}] = append(cache[Req{}], output)
	return []interface{}{output}, nil
}
