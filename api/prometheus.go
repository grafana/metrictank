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
	"github.com/grafana/metrictank/consolidation"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/util"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/pkg/timestamp"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
	"github.com/raintank/worldping-api/pkg/log"
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
	errorNone        errorType = ""
	errorTimeout     errorType = "timeout"
	errorCanceled    errorType = "canceled"
	errorExec        errorType = "execution"
	errorBadData     errorType = "bad_data"
	errorInternal    errorType = "internal"
	errorUnavailable errorType = "unavailable"
)

type prometheusQueryResult struct {
	Status    status      `json:"status"`
	Data      interface{} `json:"data,omitempty"`
	ErrorType errorType   `json:"errorType,omitempty"`
	Error     string      `json:"error,omitempty"`
}

type prometheusQueryData struct {
	ResultType promql.ValueType `json:"resultType"`
	Result     promql.Value     `json:"result"`
}

type querier struct {
	Server
	from  uint32
	to    uint32
	OrgID int
	ctx   context.Context
}

func (s *Server) labelValues(ctx *middleware.Context) {
	name := ctx.Params(":name")

	if !model.LabelNameRE.MatchString(name) {
		response.Write(ctx, response.NewError(http.StatusBadRequest, fmt.Sprintf("invalid label name: %v", name)))
		return
	}

	q, err := s.Querier(context.WithValue(context.Background(), orgID("org-id"), ctx.OrgId), 0, 0)

	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, fmt.Sprintf("unable to create queryable: %v", err)))
		return
	}
	defer q.Close()

	vals, err := q.LabelValues(name)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, fmt.Sprintf("error: %v", err)))
		return
	}

	response.Write(ctx, response.NewJson(200, prometheusQueryResult{Status: "success", Data: vals}, ""))
	return
}

func (s *Server) queryRange(ctx *middleware.Context, request models.PrometheusQueryRange) {
	start, err := parseTime(request.Start)
	if err != nil {
		response.Write(ctx, response.NewJson(http.StatusInternalServerError, prometheusQueryResult{
			Status: statusError,
			Error:  fmt.Sprintf("could not parse start time: %v", err),
		}, ""))
		return
	}

	end, err := parseTime(request.End)
	if err != nil {
		response.Write(ctx, response.NewJson(http.StatusInternalServerError, prometheusQueryResult{
			Status: statusError,
			Error:  fmt.Sprintf("could not parse end time: %v", err),
		}, ""))
		return
	}

	step, err := parseDuration(request.Step)
	if err != nil {
		response.Write(ctx, response.NewJson(http.StatusInternalServerError, prometheusQueryResult{
			Status: statusError,
			Error:  fmt.Sprintf("could not parse step duration: %v", err),
		}, ""))
		return
	}

	if step <= 0 {
		response.Write(ctx, response.NewJson(http.StatusInternalServerError, prometheusQueryResult{
			Status: statusError,
			Error:  fmt.Sprintf("step value is less than or equal to zero: %v", step),
		}, ""))
		return
	}

	qry, err := s.PromQueryEngine.NewRangeQuery(request.Query, start, end, step)

	if err != nil {
		response.Write(ctx, response.NewJson(http.StatusInternalServerError, prometheusQueryResult{
			Status: statusError,
			Error:  fmt.Sprintf("query failed: %v", err),
		}, ""))
		return
	}

	newCtx := context.WithValue(ctx.Req.Context(), orgID("org-id"), ctx.OrgId)
	res := qry.Exec(newCtx)

	if res.Err != nil {
		response.Write(ctx, response.NewJson(http.StatusInternalServerError, prometheusQueryResult{
			Status: statusError,
			Error:  fmt.Sprintf("query failed: %v", res.Err),
		}, ""))
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

func (s *Server) querySeries(ctx *middleware.Context, request models.PrometheusSeriesQuery) {
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

	_, err = s.Querier(ctx.Req.Context(), timestamp.FromTime(start), timestamp.FromTime(end))
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusInternalServerError, fmt.Sprintf("query failed: %v", err)))
		return
	}

	response.Write(ctx, response.NewError(200, "test"))
}

func (s *Server) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return &querier{
		*s,
		uint32(mint / 1000), //Convert from NS to S
		uint32(maxt / 1000), //TODO abstract this out into a seperate function that is more accurate
		ctx.Value(orgID("org-id")).(int),
		ctx,
	}, nil
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

// Select returns a set of series that matches the given label matchers.
func (q *querier) Select(matchers ...*labels.Matcher) (storage.SeriesSet, error) {
	minFrom := uint32(math.MaxUint32)
	var maxTo uint32
	var target string
	var reqs []models.Req

	expressions := []string{}
	for _, matcher := range matchers {
		if matcher.Name == model.MetricNameLabel {
			matcher.Name = "name"
		}
		expressions = append(expressions, fmt.Sprintf("%s%s%s", matcher.Name, matcher.Type, matcher.Value))
	}

	series, err := q.clusterFindByTag(q.ctx, q.OrgID, expressions, 0)
	if err != nil {
		return nil, err
	}

	minFrom = util.Min(minFrom, q.from)
	maxTo = util.Max(maxTo, q.to)
	for _, s := range series {
		for _, metric := range s.Series {
			for _, archive := range metric.Defs {
				consReq := consolidation.None
				fn := mdata.Aggregations.Get(archive.AggId).AggregationMethod[0]
				cons := consolidation.Consolidator(fn)

				newReq := models.NewReq(archive.Id, archive.NameWithTags(), target, q.from, q.to, math.MaxUint32, uint32(archive.Interval), cons, consReq, s.Node, archive.SchemaId, archive.AggId)
				fmt.Printf("From: %v\n To:%v\n", newReq.From, newReq.To)
				reqs = append(reqs, newReq)
			}
		}
	}

	select {
	case <-q.ctx.Done():
		//request canceled
		return nil, fmt.Errorf("request canceled")
	default:
	}

	reqRenderSeriesCount.Value(len(reqs))
	if len(reqs) == 0 {
		return nil, fmt.Errorf("no series found")
	}

	// note: if 1 series has a movingAvg that requires a long time range extension, it may push other reqs into another archive. can be optimized later
	reqs, _, _, err = alignRequests(uint32(time.Now().Unix()), minFrom, maxTo, reqs)
	if err != nil {
		log.Error(3, "HTTP Render alignReq error: %s", err)
		return nil, err
	}
	fmt.Println(reqs)

	out, err := q.getTargets(q.ctx, reqs)
	if err != nil {
		log.Error(3, "HTTP Render %s", err.Error())
		return nil, err
	}

	seriesSet, err := SeriesToSeriesSet(out)

	return seriesSet, err
}

// LabelValues returns all potential values for a label name.
func (q *querier) LabelValues(name string) ([]string, error) {
	result, err := q.MetricIndex.FindTagValues(q.OrgID, name, "", []string{}, 0, 100000)
	if err != nil {
		return nil, err
	}
	if result == nil {
		result = []string{}
	}
	return result, nil
}

// Close releases the resources of the Querier.
func (q *querier) Close() error {
	return nil
}

func SeriesToSeriesSet(out []models.Series) (*models.PrometheusSeriesSet, error) {
	series := []storage.Series{}
	for _, metric := range out {
		fmt.Println(metric)
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
