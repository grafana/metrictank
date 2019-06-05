package models

import (
	"strconv"
	"time"
)

// ResponseWithMeta is a graphite render response with metadata
type ResponseWithMeta struct {
	Meta   RenderMeta
	Series SeriesByTarget
}

func (rwm ResponseWithMeta) MarshalJSONFast(b []byte) ([]byte, error) {
	b = append(b, `{"meta-version":"v0.1","meta":`...)
	b, _ = rwm.Meta.MarshalJSONFast(b)
	b = append(b, `,"series":`...)
	b, _ = rwm.Series.MarshalJSONFast(b)
	b = append(b, '}')
	return b, nil
}

// RenderMeta holds metadata about a render request/response
type RenderMeta struct {
	Stats stats
}

func (rm RenderMeta) MarshalJSONFast(b []byte) ([]byte, error) {
	b = append(b, `{"stats":`...)
	b, _ = rm.Stats.MarshalJSONFast(b)
	b = append(b, '}')
	return b, nil
}

type stats struct {
	ResolveSeriesDuration time.Duration `json:"executeplan.resolve-series.ms"`
	GetTargetsDuration    time.Duration `json:"executeplan.get-targets.ms"`
	PrepareSeriesDuration time.Duration `json:"executeplan.prepare-series.ms"`
	PlanRunDuration       time.Duration `json:"executeplan.plan-run.ms"`
	SeriesFetch           uint32        `json:"executeplan.series-fetch.count"`
	PointsFetch           uint32        `json:"executeplan.points-fetch.count"`
	PointsReturn          uint32        `json:"executeplan.points-return.count"`
}

func (s stats) MarshalJSONFast(b []byte) ([]byte, error) {
	b = append(b, `{"executeplan.resolve-series.ms":`...)
	b = strconv.AppendInt(b, s.ResolveSeriesDuration.Nanoseconds()/1e6, 10)
	b = append(b, `,"executeplan.get-targets.ms":`...)
	b = strconv.AppendInt(b, s.GetTargetsDuration.Nanoseconds()/1e6, 10)
	b = append(b, `,"executeplan.prepare-series.ms":`...)
	b = strconv.AppendInt(b, s.PrepareSeriesDuration.Nanoseconds()/1e6, 10)
	b = append(b, `,"executeplan.plan-run.ms":`...)
	b = strconv.AppendInt(b, s.PlanRunDuration.Nanoseconds()/1e6, 10)
	b = append(b, `,"executeplan.series-fetch.count":`...)
	b = strconv.AppendUint(b, uint64(s.SeriesFetch), 10)
	b = append(b, `,"executeplan.points-fetch.count":`...)
	b = strconv.AppendUint(b, uint64(s.PointsFetch), 10)
	b = append(b, `,"executeplan.points-return.count":`...)
	b = strconv.AppendUint(b, uint64(s.PointsReturn), 10)
	b = append(b, '}')
	return b, nil
}
