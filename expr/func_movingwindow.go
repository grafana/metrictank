package expr

import (
	"fmt"
	"math"
	"strings"

	"github.com/grafana/metrictank/batch"
	"github.com/grafana/metrictank/schema"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/consolidation"
	"github.com/raintank/dur"
)

type FuncMovingWindow struct {
	in           GraphiteFunc
	windowSize   string
	fn           string
	xFilesFactor float64

	shiftOffset uint32
}

// NewMovingWindowConstructor takes an agg string and returns a constructor function
func NewMovingWindowConstructor(name string) func() GraphiteFunc {
	return func() GraphiteFunc {
		return &FuncMovingWindow{fn: name}
	}
}

func NewMovingWindow() GraphiteFunc {
	return &FuncMovingWindow{fn: "average"}
}

func (s *FuncMovingWindow) Signature() ([]Arg, []Arg) {
	return []Arg{
			ArgSeriesList{val: &s.in},
			ArgString{key: "windowSize", val: &s.windowSize},
			ArgString{key: "func", opt: true, val: &s.fn, validator: []Validator{IsConsolFunc}},
			ArgFloat{key: "xFilesFactor", opt: true, val: &s.xFilesFactor, validator: []Validator{WithinZeroOneInclusiveInterval}},
		}, []Arg{
			ArgSeriesList{},
		}
}

func (s *FuncMovingWindow) Context(context Context) Context {
	var err error
	s.shiftOffset, err = s.getWindowSeconds(context)
	if err != nil {
		// panic? - windowSize specified as number of points
		return context
	}

	// Adjust to fetch from the shifted context.from with
	// context.to unchanged
	context.from -= s.shiftOffset

	if math.IsNaN(s.xFilesFactor) {
		s.xFilesFactor = 0.0
	}

	return context
}

func (s *FuncMovingWindow) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	aggFunc := consolidation.GetAggFunc(consolidation.FromConsolidateBy(s.fn))

	aggFuncName := fmt.Sprintf("moving%s", strings.Title(s.fn))
	formatStr := fmt.Sprintf("%s(%%s,\"%s\")", aggFuncName, s.windowSize)

	newName := func(oldName string) string {
		return fmt.Sprintf(formatStr, oldName)
	}

	outputs := make([]models.Series, 0, len(series))
	for _, serie := range series {

		serie.Target = newName(serie.Target)
		serie.QueryPatt = newName(serie.QueryPatt)
		serie.Tags = serie.CopyTagsWith(aggFuncName, s.windowSize)
		serie.QueryFrom, serie.Datapoints = aggregateOnWindow(serie, aggFunc, s.shiftOffset, s.xFilesFactor)

		outputs = append(outputs, serie)
	}

	dataMap.Add(Req{}, outputs...)
	return outputs, nil
}

func aggregateOnWindow(serie models.Series, aggFunc batch.AggFunc, interval uint32, xFilesFactor float64) (uint32, []schema.Point) {
	out := pointSlicePool.Get().([]schema.Point)

	numPoints := len(serie.Datapoints)
	serieStart, serieEnd := serie.QueryFrom, serie.QueryTo
	queryStart := uint32(serieStart + interval)

	ptIdx := 0
	for ptIdx < numPoints-1 {
		if queryStart >= serie.Datapoints[ptIdx].Ts &&
			queryStart < serie.Datapoints[ptIdx+1].Ts {
			break
		}
		ptIdx++
	}

	for ptTs, stIdx := queryStart, 0; ptTs <= serieEnd && ptIdx < numPoints && stIdx < ptIdx; stIdx++ {
		ptTs = serie.Datapoints[ptIdx].Ts

		points := serie.Datapoints[stIdx : ptIdx+1]
		aggPoint := schema.Point{Val: math.NaN(), Ts: ptTs}

		if pointsXffCheck(points, xFilesFactor) {
			aggPoint.Val = aggFunc(points)
		}

		ptIdx++
		out = append(out, aggPoint)
	}

	return queryStart, out
}

func (s *FuncMovingWindow) getWindowSeconds(context Context) (uint32, error) {

	// Discard the sign, if present, since we need to consider
	// preceding datapoints for each point on the graph
	durStr := s.windowSize
	if durStr[0] == '-' || durStr[0] == '+' {
		durStr = durStr[1:]
	}

	// Note: s.windowSize is not overwritten and will be appended as is to the
	// new Target, QueryPatt and Tags (as is the behavior on graphite)
	// This implies a window size of '-2min' or '+2min' will be captured as is.
	// If this is unwanted behavior uncomment line below
	// s.windowSize = durStr

	// Returns the duration in seconds if no errors encountered
	// Possible errors based on validation above:
	// 	errUnknownTimeUnit: if duration is specified as number of points
	// 	or Atoi conversion errors
	shiftOffset, err := dur.ParseDuration(durStr)
	if err != nil {
		return 0, err
	}

	return shiftOffset, nil
}
