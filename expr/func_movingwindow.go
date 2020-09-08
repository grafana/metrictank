package expr

import (
	"fmt"
	"math"
	"strings"

	"github.com/grafana/metrictank/batch"
	"github.com/grafana/metrictank/errors"
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
	if s.fn == "" {
		return []Arg{
				ArgSeriesList{val: &s.in},
				ArgString{key: "windowSize", val: &s.windowSize, validator: []Validator{IsSignedIntervalString}},
				ArgString{key: "func", opt: true, val: &s.fn, validator: []Validator{IsConsolFunc}},
				ArgFloat{key: "xFilesFactor", opt: true, val: &s.xFilesFactor, validator: []Validator{WithinZeroOneInclusiveInterval}},
			}, []Arg{
				ArgSeriesList{},
			}
	}

	return []Arg{
			ArgSeriesList{val: &s.in},
			ArgString{key: "windowSize", val: &s.windowSize, validator: []Validator{IsSignedIntervalString}},
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

func aggregateOnWindow(serie models.Series, aggFunc batch.AggFunc, shiftOffset uint32, xFilesFactor float64) (uint32, []schema.Point, error) {
	out := pointSlicePool.Get().([]schema.Point)

	numPoints := len(serie.Datapoints)
	serieStart, serieEnd := serie.QueryFrom, serie.QueryTo
	queryStart := uint32(serieStart + shiftOffset)

	// Sanity check to ensure that queryStart is between serieStart and
	// serieEnd for the loop below to work (this should always be the case)"
	if queryStart < serieStart || queryStart > serieEnd {
		return 0, nil, errors.NewInternalf("query start %d doesn't lie within serie's intervals [%d,%d] ", queryStart, serieStart, serieEnd)
	}

	// if queryStart is in between points get the index of the point after
	// else get the index of the point it lies on
	ptIdx := 0
	for ptIdx < numPoints && serie.Datapoints[ptIdx].Ts < queryStart {
		ptIdx++
	}

	stIdx := 0
	stTs := serie.Datapoints[stIdx].Ts
	for ptTs := queryStart; ptTs <= serieEnd && ptIdx < numPoints; ptIdx++ {
		ptTs = serie.Datapoints[ptIdx].Ts

		// increment start index only if the difference between current and start times
		// becomes greater than the windowSize (captured in shiftOffset)
		for stIdx <= ptIdx && ptTs-stTs > shiftOffset {
			stIdx++
			stTs = serie.Datapoints[stIdx].Ts
		}

		points := serie.Datapoints[stIdx:ptIdx]
		aggPoint := schema.Point{Val: math.NaN(), Ts: ptTs}

		if pointsXffCheck(points, xFilesFactor) {
			aggPoint.Val = aggFunc(points)
		}

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

	// Input validation and operations above check for empty or signed widowSizes
	// or for widowSize specified as points. Errors unlikely here
	shiftOffset, err := dur.ParseDuration(durStr)
	if err != nil {
		return 0, err
	}

	return shiftOffset, nil
}
