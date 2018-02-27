package api

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/metrictank/api/middleware"
	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/api/response"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	schema "gopkg.in/raintank/schema.v1"
)

type orgID string
type status string

const (
	statusSuccess status = "success"
	statusError   status = "error"
)

type errorType string

const (
	errorTimeout  errorType = "timeout"
	errorCanceled errorType = "canceled"
	errorExec     errorType = "execution"
	errorBadData  errorType = "bad_data"
	errorInternal errorType = "internal"
)

type prometheusQueryResult struct {
	Status    status      `json:"status"`
	Data      interface{} `json:"data,omitempty"`
	ErrorType errorType   `json:"errorType,omitempty"`
	Error     error       `json:"error,omitempty"`
}

type prometheusQueryData struct {
	ResultType promql.ValueType `json:"resultType"`
	Result     promql.Value     `json:"result"`
}

func (s *Server) prometheusLabelValues(ctx *middleware.Context) {
	name := ctx.Params(":name")

	if !model.LabelNameRE.MatchString(name) {
		response.Write(ctx, response.NewJson(http.StatusInternalServerError, prometheusQueryResult{
			Status:    statusError,
			Error:     fmt.Errorf("unable to create label name: %v", name),
			ErrorType: errorExec,
		}, ""))
		return
	}

	q := NewQuerier(ctx.Req.Context(), s, 0, 0, ctx.OrgId, false)

	defer q.Close()
	vals, err := q.LabelValues(name)
	if err != nil {
		response.Write(ctx, response.NewJson(http.StatusInternalServerError, prometheusQueryResult{
			Status:    statusError,
			Error:     fmt.Errorf("query failed: %v", err),
			ErrorType: errorExec,
		}, ""))
		return
	}

	response.Write(ctx, response.NewJson(200, prometheusQueryResult{Status: "success", Data: vals}, ""))
	return
}

func (s *Server) prometheusQueryRange(ctx *middleware.Context, request models.PrometheusRangeQuery) {
	start, err := parseTime(request.Start)
	if err != nil {
		response.Write(ctx, response.NewJson(http.StatusInternalServerError, prometheusQueryResult{
			Status:    statusError,
			Error:     fmt.Errorf("could not parse start time: %v", err),
			ErrorType: errorBadData,
		}, ""))
		return
	}

	end, err := parseTime(request.End)
	if err != nil {
		response.Write(ctx, response.NewJson(http.StatusInternalServerError, prometheusQueryResult{
			Status:    statusError,
			Error:     fmt.Errorf("could not parse end time: %v", err),
			ErrorType: errorBadData,
		}, ""))
		return
	}

	step, err := parseDuration(request.Step)
	if err != nil {
		response.Write(ctx, response.NewJson(http.StatusInternalServerError, prometheusQueryResult{
			Status:    statusError,
			Error:     fmt.Errorf("could not parse step duration: %v", err),
			ErrorType: errorBadData,
		}, ""))
		return
	}

	if step <= 0 {
		response.Write(ctx, response.NewJson(http.StatusInternalServerError, prometheusQueryResult{
			Status:    statusError,
			Error:     fmt.Errorf("step value is less than or equal to zero: %v", step),
			ErrorType: errorBadData,
		}, ""))
		return
	}

	qry, err := s.PromQueryEngine.NewRangeQuery(request.Query, start, end, step)

	if err != nil {
		response.Write(ctx, response.NewJson(http.StatusInternalServerError, prometheusQueryResult{
			Status:    statusError,
			Error:     fmt.Errorf("query failed: %v", err),
			ErrorType: errorExec,
		}, ""))
		return
	}

	newCtx := context.WithValue(ctx.Req.Context(), orgID("org-id"), ctx.OrgId)
	res := qry.Exec(newCtx)

	if res.Err != nil {
		if res.Err != nil {
			switch res.Err.(type) {
			case promql.ErrQueryCanceled:
				response.Write(ctx, response.NewJson(http.StatusInternalServerError, prometheusQueryResult{
					Status:    statusError,
					Error:     fmt.Errorf("query failed: %v", res.Err),
					ErrorType: errorCanceled,
				}, ""))
			case promql.ErrQueryTimeout:
				response.Write(ctx, response.NewJson(http.StatusInternalServerError, prometheusQueryResult{
					Status:    statusError,
					Error:     fmt.Errorf("query failed: %v", res.Err),
					ErrorType: errorTimeout,
				}, ""))
			case promql.ErrStorage:
				response.Write(ctx, response.NewJson(http.StatusInternalServerError, prometheusQueryResult{
					Status:    statusError,
					Error:     fmt.Errorf("query failed: %v", res.Err),
					ErrorType: errorInternal,
				}, ""))
			}
			response.Write(ctx, response.NewJson(http.StatusInternalServerError, prometheusQueryResult{
				Status:    statusError,
				Error:     fmt.Errorf("query failed: %v", res.Err),
				ErrorType: errorExec,
			}, ""))
		}
		return
	}

	response.Write(ctx, response.NewJson(200,
		prometheusQueryResult{
			Data: prometheusQueryData{
				ResultType: res.Value.Type(),
				Result:     res.Value,
			},
			Status: statusSuccess,
		},
		"",
	))
}

func (s *Server) prometheusQueryInstant(ctx *middleware.Context, request models.PrometheusQueryInstant) {
	ts, err := parseTime(request.Time)
	if err != nil {
		response.Write(ctx, response.NewJson(http.StatusInternalServerError, prometheusQueryResult{
			Status:    statusError,
			Error:     fmt.Errorf("could not parse ts time: %v", err),
			ErrorType: errorBadData,
		}, ""))
		return
	}

	qry, err := s.PromQueryEngine.NewInstantQuery(request.Query, ts)

	if err != nil {
		response.Write(ctx, response.NewJson(http.StatusInternalServerError, prometheusQueryResult{
			Status:    statusError,
			Error:     fmt.Errorf("query failed: %v", err),
			ErrorType: errorExec,
		}, ""))
		return
	}

	newCtx := context.WithValue(ctx.Req.Context(), orgID("org-id"), ctx.OrgId)
	res := qry.Exec(newCtx)

	if res.Err != nil {
		if res.Err != nil {
			switch res.Err.(type) {
			case promql.ErrQueryCanceled:
				response.Write(ctx, response.NewJson(http.StatusInternalServerError, prometheusQueryResult{
					Status:    statusError,
					Error:     fmt.Errorf("query failed: %v", res.Err),
					ErrorType: errorCanceled,
				}, ""))
			case promql.ErrQueryTimeout:
				response.Write(ctx, response.NewJson(http.StatusInternalServerError, prometheusQueryResult{
					Status:    statusError,
					Error:     fmt.Errorf("query failed: %v", res.Err),
					ErrorType: errorTimeout,
				}, ""))
			case promql.ErrStorage:
				response.Write(ctx, response.NewJson(http.StatusInternalServerError, prometheusQueryResult{
					Status:    statusError,
					Error:     fmt.Errorf("query failed: %v", res.Err),
					ErrorType: errorInternal,
				}, ""))
			}
			response.Write(ctx, response.NewJson(http.StatusInternalServerError, prometheusQueryResult{
				Status:    statusError,
				Error:     fmt.Errorf("query failed: %v", res.Err),
				ErrorType: errorExec,
			}, ""))
		}
		return
	}

	response.Write(ctx, response.NewJson(200,
		prometheusQueryResult{
			Data: prometheusQueryData{
				ResultType: res.Value.Type(),
				Result:     res.Value,
			},
			Status: statusSuccess,
		},
		"",
	))
}

func (s *Server) prometheusQuerySeries(ctx *middleware.Context, request models.PrometheusSeriesQuery) {
	start, err := parseTime(request.Start)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, fmt.Sprintf("could not parse start time: %v", err)))
		return
	}

	end, err := parseTime(request.End)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, fmt.Sprintf("could not parse end time: %v", err)))
		return
	}

	q := NewQuerier(ctx.Req.Context(), s, uint32(start.Unix()), uint32(end.Unix()), ctx.OrgId, true)

	var matcherSets [][]*labels.Matcher
	for _, s := range request.Match {
		matchers, err := promql.ParseMetricSelector(s)
		if err != nil {
			response.Write(ctx, response.NewJson(http.StatusInternalServerError, prometheusQueryResult{
				Status:    statusError,
				Error:     fmt.Errorf("query failed: %v", err),
				ErrorType: errorBadData,
			}, ""))
			return
		}
		matcherSets = append(matcherSets, matchers)
	}

	var sets []storage.SeriesSet
	for _, mset := range matcherSets {
		s, err := q.Select(mset...)
		if err != nil {
			response.Write(ctx, response.NewJson(http.StatusInternalServerError, prometheusQueryResult{
				Status:    statusError,
				Error:     fmt.Errorf("query failed: %v", err),
				ErrorType: errorExec,
			}, ""))
			return
		}
		sets = append(sets, s)
	}

	set := storage.NewMergeSeriesSet(sets)
	metrics := []labels.Labels{}
	for set.Next() {
		metrics = append(metrics, set.At().Labels())
	}
	if set.Err() != nil {
		response.Write(ctx, response.NewJson(http.StatusInternalServerError, prometheusQueryResult{
			Status:    statusError,
			Error:     fmt.Errorf("query failed: %v", err),
			ErrorType: errorExec,
		}, ""))
		return
	}

	response.Write(ctx, response.NewJson(200,
		prometheusQueryResult{
			Data:   metrics,
			Status: statusSuccess,
		},
		"",
	))

	return
}

func parseTime(s string) (time.Time, error) {
	if t, err := strconv.ParseFloat(s, 64); err == nil {
		s, ns := math.Modf(t)
		return time.Unix(int64(s), int64(ns*float64(time.Second))), nil
	}
	if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
		return t, nil
	}
	return time.Time{}, fmt.Errorf("cannot parse %q to a valid timestamp", s)
}

func parseDuration(s string) (time.Duration, error) {
	if d, err := strconv.ParseFloat(s, 64); err == nil {
		ts := d * float64(time.Second)
		if ts > float64(math.MaxInt64) || ts < float64(math.MinInt64) {
			return 0, fmt.Errorf("cannot parse %q to a valid duration. It overflows int64", s)
		}
		return time.Duration(ts), nil
	}
	if d, err := model.ParseDuration(s); err == nil {
		return time.Duration(d), nil
	}
	return 0, fmt.Errorf("cannot parse %q to a valid duration", s)
}

func SeriesToSeriesSet(out []models.Series) (*models.PrometheusSeriesSet, error) {
	series := []storage.Series{}
	for _, metric := range out {
		series = append(series, models.NewPrometheusSeries(buildTagSet(metric.Target), dataPointsToPrometheusSamplePairs(metric.Datapoints)))
	}
	return models.NewPrometheusSeriesSet(series), nil
}

func dataPointsToPrometheusSamplePairs(data []schema.Point) []model.SamplePair {
	samples := []model.SamplePair{}
	for _, point := range data {
		if math.IsNaN(point.Val) {
			continue
		}
		samples = append(samples, model.SamplePair{
			Timestamp: model.Time(int64(point.Ts) * 1000),
			Value:     model.SampleValue(point.Val),
		})
	}
	return samples
}

func BuildMetadataSeriesSet(seriesNames []Series) (*models.PrometheusSeriesSet, error) {
	series := []storage.Series{}
	for _, s := range seriesNames {
		for _, metric := range s.Series {
			for _, archive := range metric.Defs {
				series = append(series, models.NewPrometheusSeries(buildTagSet(archive.NameWithTags()), []model.SamplePair{}))
			}
		}
	}
	return models.NewPrometheusSeriesSet(series), nil
}

// Turns graphite target name into prometheus graphite name
// TODO models.Series should provide a map of tags but the one returned from getTargets doesn't
func buildTagSet(name string) map[string]string {
	labelMap := map[string]string{}
	tags := strings.Split(name, ";")
	labelMap["__name__"] = tags[0]
	for _, lbl := range tags[1:] {
		kv := strings.Split(lbl, "=")
		labelMap[kv[0]] = kv[1]
	}
	return labelMap
}
