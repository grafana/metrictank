package expr

import (
	"fmt"
	"math"

	"github.com/grafana/metrictank/internal/batch"
	"github.com/grafana/metrictank/internal/consolidation"
	"github.com/grafana/metrictank/internal/schema"
	"github.com/grafana/metrictank/pkg/api/models"
	"github.com/raintank/dur"
)

type FuncSummarize struct {
	in             GraphiteFunc
	intervalString string
	fn             string
	alignToFrom    bool
}

func NewSummarize() GraphiteFunc {
	return &FuncSummarize{fn: "sum", alignToFrom: false}
}

func (s *FuncSummarize) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesList{val: &s.in},
		ArgString{val: &s.intervalString, validator: []Validator{IsIntervalString}},
		ArgString{key: "func", opt: true, val: &s.fn, validator: []Validator{IsConsolFunc}},
		ArgBool{key: "alignToFrom", opt: true, val: &s.alignToFrom},
	}, []Arg{ArgSeriesList{}}
}

func (s *FuncSummarize) Context(context Context) Context {
	context.MDP = 0
	context.PNGroup = 0
	context.consol = 0
	return context
}

func (s *FuncSummarize) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	interval, _ := dur.ParseDuration(s.intervalString)
	// note that s.fn has already been validated at series construction time using consolidation.IsConsolFunc
	aggFunc := consolidation.GetAggFunc(consolidation.FromConsolidateBy(s.fn))

	var alignToFromTarget string
	if s.alignToFrom {
		alignToFromTarget = ", true"
	}
	newName := func(oldName string) string {
		return fmt.Sprintf("summarize(%s, \"%s\", \"%s\"%s)", oldName, s.intervalString, s.fn, alignToFromTarget)
	}

	var outputs []models.Series
	for _, serie := range series {
		var newStart, newEnd uint32 = serie.QueryFrom, serie.QueryTo
		if len(serie.Datapoints) > 0 {
			newStart = serie.Datapoints[0].Ts
			newEnd = serie.Datapoints[len(serie.Datapoints)-1].Ts + serie.Interval
		}
		if !s.alignToFrom {
			newStart = newStart - (newStart % interval)
			newEnd = newEnd - (newEnd % interval) + interval
		}

		out := summarizeValues(serie, aggFunc, interval, newStart, newEnd)

		output := models.Series{
			Target:       newName(serie.Target),
			QueryPatt:    newName(serie.QueryPatt),
			QueryFrom:    serie.QueryFrom,
			QueryTo:      serie.QueryTo,
			QueryMDP:     serie.QueryMDP,
			QueryPNGroup: serie.QueryPNGroup,
			Tags:         serie.CopyTagsWith("summarize", s.intervalString),
			Datapoints:   out,
			Interval:     interval,
			Meta:         serie.Meta,
		}
		output.Tags["summarizeFunction"] = s.fn

		outputs = append(outputs, output)
		dataMap.Add(Req{}, output)
	}
	return outputs, nil
}

func summarizeValues(serie models.Series, aggFunc batch.AggFunc, interval, start, end uint32) []schema.Point {
	numPoints := len(serie.Datapoints)
	out := pointSlicePool.GetMin(numPoints)

	// graphite-compatible bit

	for ts, i := start, 0; i < numPoints && ts < end; ts += interval {
		s := i
		for ; i < numPoints && serie.Datapoints[i].Ts < ts+interval; i++ {
			if serie.Datapoints[i].Ts <= ts {
				s = i
			}
		}

		aggPoint := schema.Point{Val: math.NaN(), Ts: ts}
		if s != i {
			aggPoint.Val = aggFunc(serie.Datapoints[s:i])
		}

		out = append(out, aggPoint)
	}

	// MT specific bit: if !s.alignToFrom we want the output to be canonical
	// only thing needed is strip out the first point if its TS < from
	if len(out) != 0 && out[0].Ts < serie.QueryFrom {
		out = out[1:]
	}

	return out
}
