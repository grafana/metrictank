package expr

import (
	"fmt"
	"math"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/errors"
	"github.com/grafana/metrictank/schema"
)

// FuncDivideSeriesLists divides dividends by divisors, pairwise
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
	// note: like FuncDivideSeries, this is an aggregation function (turning pairs of series into one)
	// unlike FuncDivideSeries, we don't use any input series more than once,
	// thus any already proposed pre-normalization can proceed as planned
	// and hence do not have to reset PNGroup.
	// if anything, in some exotic cases divisors (and dividends) may have different intervals amongst themselves
	// but matching intervals when we pair up a divisor with a dividend, in which case we could technically introduce pre-normalization
	// but we can't really predict that here, so let's not worry about that.
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
		return nil, errors.NewBadRequest("dividendSeriesList and divisorSeriesList argument must have equal length")
	}

	var series []models.Series
	for i := range dividends {
		dividend, divisor := NormalizeTwo(cache, dividends[i], divisors[i])

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
			QueryMDP:     dividend.QueryMDP,
			QueryPNGroup: dividend.QueryPNGroup,
			Meta:         dividend.Meta.Copy().Merge(divisor.Meta),
		}
		cache[Req{}] = append(cache[Req{}], output)
		series = append(series, output)
	}
	return series, nil
}
