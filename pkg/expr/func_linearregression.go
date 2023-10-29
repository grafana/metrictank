package expr

import (
	"fmt"
	"math"
	"time"

	"github.com/grafana/metrictank/internal/mdata"
	"github.com/grafana/metrictank/internal/schema"
	"github.com/grafana/metrictank/pkg/api/models"
	"github.com/grafana/metrictank/pkg/api/tz"
)

type FuncLinearRegression struct {
	in GraphiteFunc

	startSourceAt string // render time format
	endSourceAt   string
	startSource   uint32 // epoch seconds
	endSource     uint32

	startTarget uint32 // epoch seconds
	endTarget   uint32
}

func NewLinearRegression() GraphiteFunc {
	return &FuncLinearRegression{}
}

func (s *FuncLinearRegression) Signature() ([]Arg, []Arg) {
	return []Arg{
			ArgSeriesList{
				val: &s.in,
			},
			ArgString{
				key:       "startSourceAt",
				opt:       true,
				validator: []Validator{IsRenderTimeFormat},
				val:       &s.startSourceAt,
			},
			ArgString{
				key:       "endSourceAt",
				opt:       true,
				validator: []Validator{IsRenderTimeFormat},
				val:       &s.endSourceAt,
			},
		},
		[]Arg{ArgSeriesList{}}
}

func (s *FuncLinearRegression) Context(context Context) Context {
	s.startTarget = context.from
	s.endTarget = context.to

	now := time.Now()
	defaultFrom := uint32(now.Add(-5 * time.Minute).Unix())
	defaultTo := uint32(now.Unix())
	var err error
	s.startSource, s.endSource, err = tz.GetFromTo(tz.FromTo{
		From: s.startSourceAt,
		To:   s.endSourceAt,
	}, now, defaultFrom, defaultTo)
	if err != nil {
		return context // todo panic?
	}
	context.from = s.startSource
	context.to = s.endSource

	return context
}

func (s *FuncLinearRegression) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}

	results := []models.Series{}
	for _, serie := range series {
		factor, offset, isValid := linearRegressionAnalysis(serie)
		if !isValid {
			continue
		}

		normalizedStartTarget := mdata.AggBoundary(s.startTarget, serie.Interval)
		size := int((s.endTarget-normalizedStartTarget)/serie.Interval + 1)
		datapoints := pointSlicePool.GetMin(size)
		for i := 0; i < size; i++ {
			datapoint := schema.Point{
				Val: offset + (float64(normalizedStartTarget)+float64(i)*float64(serie.Interval))*factor,
				Ts:  normalizedStartTarget + uint32(i)*serie.Interval,
			}
			datapoints = append(datapoints, datapoint)
		}

		newSeries := serie.Copy([]schema.Point{})
		newSeries.Target = fmt.Sprintf("linearRegression(%s, %d, %d)", serie.Target, s.startSource, s.endSource)
		newSeries.Datapoints = datapoints
		newSeries.Tags["linearRegressions"] = fmt.Sprintf("%d, %d", s.startSource, s.endSource)
		newSeries.QueryPatt = newSeries.Target
		newSeries.QueryFrom = s.startTarget
		newSeries.QueryTo = s.endTarget

		results = append(results, newSeries)
	}

	dataMap.Add(Req{}, results...)
	return results, nil
}

func linearRegressionAnalysis(series models.Series) (float64, float64, bool) {
	// Some functions normalize the series' datapoint timestamps,
	// but not the series' QueryFrom/QueryTo fields.
	startSource := series.QueryFrom
	if len(series.Datapoints) > 0 {
		startSource = series.Datapoints[0].Ts
	}

	var n float64
	var sumI float64
	var sumII float64
	var sumV float64
	var sumIV float64
	for i, point := range series.Datapoints {
		if math.IsNaN(point.Val) {
			continue
		}

		n++
		sumI += float64(i)
		sumII += float64(i) * float64(i)
		sumV += point.Val
		sumIV += float64(i) * point.Val
	}

	denominator := n*sumII - sumI*sumI
	if denominator == 0 {
		return 0, 0, false
	}
	factor := (n*sumIV - sumI*sumV) / denominator / float64(series.Interval)
	offset := (sumII*sumV-sumIV*sumI)/denominator - factor*float64(startSource)

	return factor, offset, true
}
