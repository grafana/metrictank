package expr

import (
	"fmt"
	"math"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/errors"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/util"
)

// FuncDivideSeries divides 1-N dividend series by 1 dividend series
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

	var series []models.Series
	// if len(dividends) > 1 the same divisor series will be used multiple times,
	// and we'll possibly need to normalize it to different intervals (if the dividends have differing intervals)
	// (we also need to normalize if there's only 1 dividend but it has a different interval than the divisor)
	// so let's keep track of the different "versions" of the divisor that we have available.
	// (the dividend(s) may also need to be normalized but we only use them once so they don't require special attention)
	divisorsByRes := make(map[uint32]models.Series)
	divisorsByRes[divisors[0].Interval] = divisors[0]
	for _, dividend := range dividends {
		out := pointSlicePool.Get().([]schema.Point)
		divisor := divisors[0]
		if dividend.Interval != divisors[0].Interval {
			lcm := util.Lcm([]uint32{dividend.Interval, divisor.Interval})
			newDiv, ok := divisorsByRes[lcm]
			if ok {
				divisor = newDiv
				// we now have the right divisor but may still need to normalize the dividend
				dividend, divisor = normalizeTwo(cache, dividend, divisor)
			} else {
				dividend, divisor = normalizeTwo(cache, dividend, divisor)
				divisorsByRes[lcm] = divisor
			}
		}
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
			QueryPNGroup: dividend.QueryPNGroup,
			QueryMDP:     dividend.QueryMDP,
			Meta:         dividend.Meta.Copy().Merge(divisor.Meta),
		}
		cache[Req{}] = append(cache[Req{}], output)
		series = append(series, output)
	}
	return series, nil
}
