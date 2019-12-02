package expr

import (
	"fmt"
	"math"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/errors"
	"github.com/grafana/metrictank/schema"
)

type FuncDivideSeries struct {
	dividend GraphiteFunc
	divisor  GraphiteFunc
}

func NewDivideSeries() GraphiteFunc {
	return &FuncDivideSeries{}
}

func (s *FuncDivideSeries) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesList{val: &s.dividend},
		ArgSeries{val: &s.divisor},
	}, []Arg{ArgSeries{}}
}

func (s *FuncDivideSeries) Context(context Context) Context {
	// note: technically divideSeries() is a sort of aggregation function
	// that "aggregates" each dividend series together with the divisor.
	// thus, it's theoretically possible to apply pre-normalization
	// but, if it receives dividend series that may differ in their interval, then
	// this would be really hard to juggle because the divisor would get used
	// multiple times with different intervals.
	// so to be safe, let's just treat divideSeries like an opaque aggregation and
	// cancel out ongoing pre-normalization.
	// we wouldn't want to pre-normalize all dividends and the divisor to 1 common
	// interval because that could cause coarse dividends to affect (coarsen)
	// their fellow dividends
	context.PNGroup = 0
	return context
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
		return nil, errors.NewBadRequestf("need 1 divisor series, not %d", len(divisors))
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

		name := fmt.Sprintf("divideSeries(%s,%s)", dividend.Target, divisor.Target)
		output := models.Series{
			Target:       name,
			QueryPatt:    name,
			Tags:         map[string]string{"name": name},
			Datapoints:   out,
			Interval:     divisor.Interval,
			Consolidator: dividend.Consolidator,
			QueryCons:    dividend.QueryCons,
			QueryFrom:    dividend.QueryFrom,
			QueryTo:      dividend.QueryTo,
			Meta:         dividend.Meta.Copy().Merge(divisor.Meta),
		}
		cache[Req{}] = append(cache[Req{}], output)
		series = append(series, output)
	}
	return series, nil
}
