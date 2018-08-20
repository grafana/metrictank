package expr

import (
	"errors"
	"fmt"
	"math"

	"github.com/grafana/metrictank/api/models"
	"github.com/raintank/schema"
)

type FuncDivideSeriesLists struct {
	dividends GraphiteFunc
	divisors  GraphiteFunc
}

func NewDivideSeriesLists() GraphiteFunc {
	return &FuncDivideSeriesLists{}
}

func (s *FuncDivideSeriesLists) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesList{val: &s.dividends},
		ArgSeriesList{val: &s.divisors},
	}, []Arg{ArgSeries{}}
}

func (s *FuncDivideSeriesLists) Context(context Context) Context {
	return context
}

func (s *FuncDivideSeriesLists) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	dividends, err := s.dividends.Exec(cache)
	if err != nil {
		return nil, err
	}
	divisors, err := s.divisors.Exec(cache)
	if err != nil {
		return nil, err
	}
	if len(divisors) != len(dividends) {
		return nil, errors.New("dividendSeriesList and divisorSeriesList argument must have equal length")
	}

	var series []models.Series
	for i, dividend := range dividends {
		divisor := divisors[i]
		out := pointSlicePool.Get().([]schema.Point)
		for i := 0; i < len(dividend.Datapoints); i++ {
			p := schema.Point{
				Ts: dividend.Datapoints[i].Ts,
			}
			if divisor.Datapoints[i].Val == 0 {
				p.Val = math.NaN()
			} else {
				p.Val = dividend.Datapoints[i].Val / divisor.Datapoints[i].Val
			}
			out = append(out, p)
		}

		name := fmt.Sprintf("divideSeries(%s,%s)", dividend.Target, divisor.Target)
		output := models.Series{
			Target:       name,
			QueryPatt:    name,
			Tags:         map[string]string{"name": name},
			Datapoints:   out,
			Interval:     divisor.Interval,
			Consolidator: dividend.Consolidator,
			QueryCons:    dividend.QueryCons,
		}
		cache[Req{}] = append(cache[Req{}], output)
		series = append(series, output)
	}
	return series, nil
}
