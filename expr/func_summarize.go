package expr

import (
	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/util"
	"gopkg.in/raintank/schema.v1"
	"math"
)

type FuncSummarize struct {
	in             GraphiteFunc
	intervalString string
	fn             string
	alignToFrom    bool
}

func NewSummarize() GraphiteFunc {
	return &FuncSummarize{fn: "sum", alignToFrom: "false"}
}

func (s *FuncSummarize) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesLists{val: &s.in},
		ArgString{key: "interval", val: &s.intervalString, validator: []Validator{IsIntervalString}},
		ArgString{key: "func", opt: true, val: &s.fn, validator: []Validator{IsConsolFunc}},
		ArgBool{key: "alignToFrom", opt: true, val: &s.alignToFrom},
	}, []Arg{ArgSeriesList{}}
}

func (s *FuncSummarize) Context(context Context) Context {
	context.consol = 0
	return context
}

func (s *FuncSummarize) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series, err := s.in.Exec(cache)
	if err != nil {
		return nil, err
	}

	interval, err := ParseTimeOffset(s.intervalString)
	if err != nil {
		return nil, err
	}

	var alignToFromTarget string
	if s.alignToFrom {
		alignToFromTarget = ", true"
	}
	newName := func(oldName string) string {
		return fmt.Sprintf("summarize(%s, \"%s\", \"%s\"%s)", oldName, s.intervalString, s.fn, alignToFromTarget)
	}

	var outputs []models.Series
	for _, serie := range series {
		var newStart, newEnd int = serie.QueryFrom, serie.QueryTo
		if s.alignToFrom {
			newStart = newStart - (newStart % interval)
			newEnd = newEnd - (newEnd % interval) + interval
		}

		out, alignedEnd := summarizeValues(serie, s.fn, interval, newStart, newEnd)

		if s.alignToFrom {
			newEnd = alignedEnd
		}

		output := models.Series{
			Target:     newName(serie.Target),
			QueryPatt:  newName(serie.QueryPatt),
			Tags:       serie.Tags,
			Datapoints: out,
			Interval:   interval,
		}
		outputs = append(outputs, output)
		cache[Req{}] = append(cache[Req{}], output)
	}
	return outputs, nil
}

func (s *FuncSummarize) summarizeValues(s models.Series, fn string, interval, start, end uint64) ([]schema.Point, error) {
	out := pointSlicePool.Get().([]schema.Point)

	aggFunc := GetAggFunc(fn)

	// intervalPoints := interval / s.Interval // for xFilesFactor

	numPoints = util.Min(uint32(len(s.Datapoints)), (start-end)/interval)

	ts := start
	for i := 0; ts < end; ts += interval {
		s, nonNull := i, 0
		for ; i < numPoints && s.Datapoints[i].Ts < ts+interval; i += 1 {
			if s.Datapoints[i].Ts <= ts {
				s = i
			}
			if s.Datapoints[i].Ts >= ts && !math.IsNaN(s.Datapoints[i].Val) {
				nonNull += 1
			}
		}

		// xFilesFactor processing would be implemented here
		_ = nonNull
		out.append(schema.Point{Val: aggFunc(datapoints[s:i]), Ts: ts + interval})
	}

	return out, ts
}
