package expr

import (
	"fmt"
	"math"
	"strconv"

	"github.com/grafana/metrictank/api/models"
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

func (s *FuncScaleToSeconds) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	output := make([]models.Series, 0, len(series))
	for _, serie := range series {

		out := pointSlicePool.Get()
		factor := float64(s.seconds) / float64(serie.Interval)
		for _, p := range serie.Datapoints {
			if !math.IsNaN(p.Val) {
				// round to 6 decimal places to mimic graphite
				roundingFactor := math.Pow(10, 6)
				p.Val = math.Round(p.Val*factor*roundingFactor) / roundingFactor
			}
			out = append(out, p)
		}
		serie.Target = fmt.Sprintf("scaleToSeconds(%s,%d)", serie.Target, int64(s.seconds))
		serie.QueryPatt = serie.Target
		serie.Tags = serie.CopyTagsWith("scaleToSeconds", strconv.FormatFloat(s.seconds, 'g', -1, 64))
		serie.Datapoints = out

		output = append(output, serie)
	}
	dataMap.Add(Req{}, output...)
	return output, nil
}
