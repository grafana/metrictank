package expr

import (
	"errors"
	"fmt"
	"math"
	"strings"

	"github.com/grafana/metrictank/batch"
	"github.com/grafana/metrictank/schema"
	log "github.com/sirupsen/logrus"

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
	return &FuncMovingWindow{fn: "average", xFilesFactor: 0}
}

func (s *FuncMovingWindow) Signature() ([]Arg, []Arg) {
	return []Arg{
			ArgSeriesList{val: &s.in},
			ArgString{key: "windowSize", val: &s.windowSize, validator: []Validator{IsSignedIntervalString}},
			ArgString{key: "func", opt: true, val: &s.fn, validator: []Validator{IsConsolFunc}},
			ArgFloat{key: "xFilesFactor", opt: true, val: &s.xFilesFactor, validator: []Validator{WithinZeroOneInclusiveInterval}},
		}, []Arg{
			ArgSeriesList{},
		}
}

func (s *FuncMovingWindow) Context(context Context) Context {
	var err error
	s.shiftOffset, err = s.getWindowSeconds()
	if err != nil {
		// shouldn't happen, validated above
		log.Warnf("movingWindow: encountered error in getWindowSeconds, %s", err)
		return context
	}

	// Adjust to fetch from the shifted context.from with
	// context.to unchanged
	context.from -= s.shiftOffset
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

		from, points, err := aggregateOnWindow(serie, aggFunc, s.shiftOffset, s.xFilesFactor)
		if err != nil {
			return nil, err
		}

		serie.Target = newName(serie.Target)
		serie.QueryPatt = newName(serie.QueryPatt)
		serie.Tags = serie.CopyTagsWith(aggFuncName, s.windowSize)
		serie.QueryFrom = from
		serie.Datapoints = points

		outputs = append(outputs, serie)
	}

	dataMap.Add(Req{}, outputs...)
	return outputs, nil
}

func aggregateOnWindow(serie models.Series, aggFunc batch.AggFunc, interval uint32, xFilesFactor float64) (uint32, []schema.Point, error) {
	out := pointSlicePool.Get().([]schema.Point)

	numPoints := len(serie.Datapoints)
	serieStart, serieEnd := serie.QueryFrom, serie.QueryTo
	queryStart := uint32(serieStart + interval)

	if queryStart < serieStart {
		return 0, nil, errors.New("query start time cannot be before the first point in series")
	}

	ptIdx := 0
	for ptIdx < numPoints-1 && serie.Datapoints[ptIdx+1].Ts <= queryStart {
		ptIdx++
	}

	for ptTs, stIdx := queryStart, 0; ptTs <= serieEnd && ptIdx < numPoints && stIdx <= ptIdx; stIdx++ {
		ptTs = serie.Datapoints[ptIdx].Ts

		points := serie.Datapoints[stIdx:ptIdx]
		aggPoint := schema.Point{Val: math.NaN(), Ts: ptTs}

		if pointsXffCheck(points, xFilesFactor) {
			aggPoint.Val = aggFunc(points)
		}

		ptIdx++
		out = append(out, aggPoint)
	}

	return queryStart, out, nil
}

func (s *FuncMovingWindow) getWindowSeconds() (uint32, error) {

	// Discard the sign, if present, since we need to consider
	// preceding datapoints for each point on the graph
	durStr := s.windowSize
	if durStr[0] == '-' || durStr[0] == '+' {
		durStr = durStr[1:]
	}

	// Note: s.windowSize is not overwritten and will be appended as is to the
	// new Target, QueryPatt and Tags (keeping it consistent with graphite)
	// This implies a window size of '-2min' or '+2min' will be captured as is.
	// However the time operation is always a lookback over preceeding points.
	// If this needs to be corrected in the native implementation
	// uncomment the line below.
	// s.windowSize = durStr

	// Input validation and operations above check for empty or signed widowSizes
	// or for widowSize specified as points. Errors unlikely here
	shiftOffset, err := dur.ParseDuration(durStr)
	if err != nil {
		return 0, err
	}

	return shiftOffset, nil
}
