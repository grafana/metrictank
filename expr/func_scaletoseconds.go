package expr

import (
	"fmt"
	"math"
	"strconv"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
)

type FuncScaleToSeconds struct {
	in      GraphiteFunc
	seconds float64
}

func NewScaleToSeconds() GraphiteFunc {
	return &FuncScaleToSeconds{}
}

func (s *FuncScaleToSeconds) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesList{val: &s.in},
		ArgFloat{key: "seconds", val: &s.seconds},
	}, []Arg{ArgSeriesList{}}
}

func (s *FuncScaleToSeconds) Context(context Context) Context {
	return context
}

func (s *FuncScaleToSeconds) Exec(dataMap map[Req][]models.Series) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	for i, serie := range series {
		series[i].Target = fmt.Sprintf("scaleToSeconds(%s,%d)", serie.Target, int64(s.seconds))
		series[i].QueryPatt = series[i].Target
		series[i].Tags = serie.CopyTagsWith("scaleToSeconds", strconv.FormatFloat(s.seconds, 'g', -1, 64))
		series[i].Datapoints = pointSlicePool.Get().([]schema.Point)

		factor := float64(s.seconds) / float64(serie.Interval)
		for _, p := range serie.Datapoints {
			if !math.IsNaN(p.Val) {
				// round to 6 decimal places to mimic graphite
				roundingFactor := math.Pow(10, 6)
				p.Val = math.Round(p.Val*factor*roundingFactor) / roundingFactor
			}
			series[i].Datapoints = append(series[i].Datapoints, p)
		}
	}
	dataMap[Req{}] = append(dataMap[Req{}], series...)
	return series, nil
}
