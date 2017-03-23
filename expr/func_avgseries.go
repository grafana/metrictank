package expr

import (
	"math"

	"github.com/raintank/metrictank/api/models"
)

type FuncAvgSeries struct {
}

func NewAvgSeries() Func {
	return FuncAvgSeries{}
}

func (s FuncAvgSeries) Signature() ([]argType, []argType) {
	return []argType{seriesList}, []argType{series}
}

func (s FuncAvgSeries) Init(args []*expr) error {
	return nil
}

func (s FuncAvgSeries) Depends(from, to uint32) (uint32, uint32) {
	return from, to
}

func (s FuncAvgSeries) Exec(in ...interface{}) ([]interface{}, error) {
	series, ok := in[0].([]models.Series)
	if !ok {
		return nil, ErrArgumentBadType
	}
	if len(series) == 1 {
		return []interface{}{series[0]}, nil
	}
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
		if num == 0 {
			series[0].Datapoints[i].Val = math.NaN()
		} else {
			series[0].Datapoints[i].Val = sum / float64(num)
		}
	}

	return []interface{}{series[0]}, nil
}
