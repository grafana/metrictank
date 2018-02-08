package api

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/grafana/metrictank/api/middleware"
	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/api/response"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/pkg/labels"
	"github.com/prometheus/prometheus/promql"
	"github.com/prometheus/prometheus/storage"
)

type OrgID string

func (s *Server) queryRange(ctx *middleware.Context, request models.PrometheusQueryRange) {
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
	step, err := parseDuration(request.Step)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusBadRequest, fmt.Sprintf("could not parse step duration: %v", err)))
		return
	}
	if step <= 0 {
		response.Write(ctx, response.NewError(http.StatusBadRequest, fmt.Sprintf("step value is less than or equal to zero: %v", err)))
		return
	}

	qry, err := s.PromQueryEngine.NewRangeQuery(request.Query, start, end, step)
	if err != nil {
		response.Write(ctx, response.NewError(http.StatusInternalServerError, fmt.Sprintf("query failed: %v", err)))
		return
	}
	res := qry.Exec(context.WithValue(context.Background(), OrgID("org-id"), ctx.OrgId))
	if res.Err != nil {
		switch res.Err.(type) {
		case promql.ErrQueryCanceled:
			response.Write(ctx, response.NewError(http.StatusInternalServerError, fmt.Sprintf("query failed: %v", res.Err)))
			return
		case promql.ErrQueryTimeout:
			response.Write(ctx, response.NewError(http.StatusInternalServerError, fmt.Sprintf("query failed: %v", res.Err)))
			return
		case promql.ErrStorage:
			response.Write(ctx, response.NewError(http.StatusInternalServerError, fmt.Sprintf("query failed: %v", res.Err)))
			return
		}
		response.Write(ctx, response.NewError(http.StatusInternalServerError, fmt.Sprintf("query failed: %v", res.Err)))
		return
	}

	response.Write(ctx, response.NewJson(200,
		models.ProemtheusQueryData{
			ResultType: res.Value.Type(),
			Result:     res.Value,
		},
		"",
	))
}

func (s *Server) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return &querier{
		*s,
		ctx.Value("org-id").(int),
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

type querier struct {
	Server
	OrgId int
}

// Select returns a set of series that matches the given label matchers.
func (q *querier) Select(...*labels.Matcher) (storage.SeriesSet, error) {
	return nil, fmt.Errorf("Select not implemented")
}

// LabelValues returns all potential values for a label name.
func (q *querier) LabelValues(name string) ([]string, error) {
	result, err := q.MetricIndex.TagDetails(q.OrgId, name, "", 0)
	if err != nil {
		return nil, err
	}
	if result == nil {
		result = make(map[string]uint64)
	}
	data := models.IndexTagDetails{OrgId: q.OrgId, Tag: name, Filter: filter, From: from}
	resps, err := s.peerQuery(ctx, data, "clusterTagDetails", "/index/tag_details", false)
	if err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		//request canceled
		return nil, nil
	default:
	}
	resp := models.IndexTagDetailsResp{}
	for _, r := range resps {
		_, err = resp.UnmarshalMsg(r.buf)
		if err != nil {
			return nil, err
		}
		for k, v := range resp.Values {
			result[k] = result[k] + v
		}
	}
	return nil, fmt.Errorf("LabelValues not implemented")
}

// Close releases the resources of the Querier.
func (q *querier) Close() error {
	return nil
}
