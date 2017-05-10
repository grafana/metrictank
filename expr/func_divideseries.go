package expr

import (
	"errors"
	"fmt"
	"math"

	"github.com/raintank/metrictank/api/models"
	"gopkg.in/raintank/schema.v1"
)

type FuncDivideSeries struct {
	dividend Func
	divisor  Func
}

func NewDivideSeries() Func {
	return &FuncDivideSeries{}
}

func (s *FuncDivideSeries) Signature() ([]arg, []arg) {
	return []arg{
		argSeriesList{val: &s.dividend},
		argSeries{val: &s.divisor},
	}, []arg{argSeries{}}
}

func (s *FuncDivideSeries) NeedRange(from, to uint32) (uint32, uint32) {
	return from, to
}

func (s *FuncDivideSeries) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	dividends, err := s.dividend.Exec(cache)
	if err != nil {
		return nil, err
	}
	divisors, err := s.divisor.Exec(cache)
	if err != nil {
		return nil, err
	}
	if len(divisors) != 1 {
		return nil, errors.New(fmt.Sprintf("need 1 divisor series, not %d", len(divisors)))
	}
	divisor := divisors[0]

	var series []models.Series
	for _, dividend := range dividends {
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
		output := models.Series{
			Target:     fmt.Sprintf("divideSeries(%s,%s)", dividend.Target, divisor.Target),
			Datapoints: out,
			Interval:   divisor.Interval,
		}
		cache[Req{}] = append(cache[Req{}], output)
		series = append(series, output)
	}
	return series, nil
}
