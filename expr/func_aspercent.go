package expr

import (
	"fmt"
	"math"

	"github.com/raintank/metrictank/api/models"
	"gopkg.in/raintank/schema.v1"
)

type errAsPercentNumSeriesMismatch struct {
	numIn    int
	numTotal int
}

func (e errAsPercentNumSeriesMismatch) Error() string {
	return fmt.Sprintf("asPercent got %d input series but %d total series (should  be same amount or 1)", e.numIn, e.numTotal)
}

type FuncAsPercent struct {
	in          []GraphiteFunc
	totalFloat  float64
	totalSeries GraphiteFunc
}

func NewAsPercent() GraphiteFunc {
	return &FuncAsPercent{
		totalFloat: math.NaN(),
	}
}

func (s *FuncAsPercent) Signature() ([]Arg, []Arg) {
	return []Arg{
			ArgSeriesLists{val: &s.in},
			ArgIn{
				key: "total",
				opt: true,
				args: []Arg{
					ArgFloat{val: &s.totalFloat},
					ArgSeriesList{val: &s.totalSeries},
				},
			},
		}, []Arg{
			ArgSeriesList{},
		}
}

func (s *FuncAsPercent) Context(context Context) Context {
	return context
}

func (s *FuncAsPercent) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	var series, outputs []models.Series
	for i := range s.in {
		serie, err := s.in[i].Exec(cache)
		if err != nil {
			return nil, err
		}
		series = append(series, serie...)
	}
	// total: - None (unspecified) -> sum of all input series
	//        - single series -> all inputs use same total series
	//        - same amount of series as input -> pairwise calculation
	//        - integer -> use same value for all points
	if !math.IsNaN(s.totalFloat) {
		// user entered 40.00 -> print as 40
		// user entered 40 -> print as 40
		// user entered 40.123 -> print as 40.123
		patt := "asPercent(%s,%f)"
		// if no decimals, print as integer
		if s.totalFloat-float64(int64(s.totalFloat)) == 0 {
			patt = "asPercent(%s,%.0f)"
		}
		for _, in := range series {
			out := pointSlicePool.Get().([]schema.Point)
			for j := 0; j < len(in.Datapoints); j++ {
				point := schema.Point{
					Ts:  in.Datapoints[j].Ts,
					Val: computeAsPercent(in.Datapoints[j].Val, s.totalFloat),
				}
				out = append(out, point)
			}
			output := models.Series{
				Target:       fmt.Sprintf(patt, in.Target, s.totalFloat),
				QueryPatt:    fmt.Sprintf(patt, in.QueryPatt, s.totalFloat),
				Datapoints:   out,
				Interval:     in.Interval,
				Consolidator: in.Consolidator,
				QueryCons:    in.QueryCons,
			}
			cache[Req{}] = append(cache[Req{}], output)
			outputs = append(outputs, output)
		}
	} else {
		var totalSeries []models.Series
		var err error
		if s.totalSeries != nil {
			totalSeries, err = s.totalSeries.Exec(cache)
			if err != nil {
				return nil, err
			}
			if len(totalSeries) != len(series) && len(totalSeries) != 1 {
				return nil, errAsPercentNumSeriesMismatch{len(series), len(totalSeries)}
			}
		} else {
			sum := NewSumSeries()
			sumIn, _ := sum.Signature()
			sumArgList := sumIn[0].(ArgSeriesLists)
			*sumArgList.val = s.in
			totalSeries, err = sum.Exec(cache)
			if err != nil {
				return nil, err
			}
		}
		for i, in := range series {
			out := pointSlicePool.Get().([]schema.Point)
			var total models.Series
			if len(totalSeries) == 1 {
				total = totalSeries[0]
			} else {
				total = totalSeries[i]
			}

			for j := 0; j < len(in.Datapoints); j++ {
				point := schema.Point{
					Ts:  in.Datapoints[j].Ts,
					Val: computeAsPercent(in.Datapoints[j].Val, total.Datapoints[j].Val),
				}
				out = append(out, point)
			}
			output := models.Series{
				Datapoints:   out,
				Interval:     in.Interval,
				Consolidator: in.Consolidator,
				QueryCons:    in.QueryCons,
			}
			if s.totalSeries != nil {
				output.Target = fmt.Sprintf("asPercent(%s,%s)", in.Target, total.Target)
				output.QueryPatt = fmt.Sprintf("asPercent(%s,%s)", in.QueryPatt, total.QueryPatt)
			} else {
				output.Target = fmt.Sprintf("asPercent(%s)", in.Target)
				output.QueryPatt = fmt.Sprintf("asPercent(%s)", in.QueryPatt)
			}
			cache[Req{}] = append(cache[Req{}], output)
			outputs = append(outputs, output)
		}
	}
	return outputs, nil
}

func computeAsPercent(in, total float64) float64 {
	if math.IsNaN(in) || math.IsNaN(total) {
		return math.NaN()
	}
	if total == 0 {
		if in == 0 {
			return math.NaN()
		}
		return math.MaxFloat64
	}
	return 100 * in / total
}
