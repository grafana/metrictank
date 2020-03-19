package expr

import (
	"fmt"
	"math"
	"math/big"
	"strconv"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
)

type FuncRound struct {
	in        GraphiteFunc
	precision int64
}

func NewRound() GraphiteFunc {
	return &FuncRound{}
}

func (s *FuncRound) Signature() ([]Arg, []Arg) {
	return []Arg{
			ArgSeriesList{val: &s.in},
			ArgInt{key: "precision", opt: true, validator: []Validator{IntPositive}, val: &s.precision},
		}, []Arg{
			ArgSeriesList{},
		}
}

func (s *FuncRound) Context(context Context) Context {
	return context
}

func (s *FuncRound) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	outputs := make([]models.Series, len(series))
	precisionMult := float64(math.Pow10(int(s.precision)))
	for i, serie := range series {
		out := pointSlicePool.Get().([]schema.Point)

		for _, v := range serie.Datapoints {
			out = append(out, schema.Point{Val: roundToPrecision(v.Val, precisionMult, int(s.precision)), Ts: v.Ts})
		}

		s := models.Series{
			Target:       fmt.Sprintf("round(%s,%d)", serie.Target, s.precision),
			QueryPatt:    fmt.Sprintf("round(%s,%d)", serie.Target, s.precision),
			Tags:         serie.CopyTagsWith("round", strconv.Itoa(int(s.precision))),
			Datapoints:   out,
			Interval:     serie.Interval,
			Meta:         serie.Meta,
			QueryMDP:     serie.QueryMDP,
			QueryPNGroup: serie.QueryPNGroup,
		}
		outputs[i] = s
	}
	dataMap.Add(Req{}, outputs...)
	return outputs, nil
}

func roundToPrecision(val float64, precisionMult float64, precision int) float64 {
	if math.IsNaN(val) || math.IsInf(val, 0) {
		return val
	}

	// Special case 1: Large negative precision, pow10 is too small to represent
	if precisionMult == 0 {
		return 0
	}
	// Special case 2: Large positive precision, pow10 is too large to represent
	if math.IsInf(precisionMult, 0) {
		return val
	}

	alignedVal := val * precisionMult
	// Special case 3: underflow or overflow, need to do expensive "Big" version
	if math.IsInf(alignedVal, 0) {
		return roundToPrecisionSlow(val, precision)
	}
	return math.Round(alignedVal) / precisionMult
}

func roundToPrecisionSlow(val float64, precision int) float64 {
	f := big.Rat{}
	f.SetFloat64(val)
	ret, err := strconv.ParseFloat(f.FloatString(precision), 64)
	if err != nil {
		return math.NaN()
	}
	return ret
}
