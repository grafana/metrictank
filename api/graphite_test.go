package api

import (
	"context"
	"encoding/binary"
	"fmt"
	"testing"
	"time"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/expr"
	"github.com/grafana/metrictank/expr/tagquery"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/jaeger"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/schema"
	opentracing "github.com/opentracing/opentracing-go"
	log "github.com/sirupsen/logrus"
)

type mockSeriesFinder struct {
	findResult []Series
}

func newMockSeriesFinder() *mockSeriesFinder {
	return &mockSeriesFinder{}
}

func (m *mockSeriesFinder) clusterFindByTag(ctx context.Context, orgId uint32, expressions tagquery.Expressions, from int64, maxSeries int, softLimit bool) ([]Series, error) {
	return m.findResult, nil
}

func (m *mockSeriesFinder) findSeries(ctx context.Context, orgId uint32, patterns []string, seenAfter int64) ([]Series, error) {
	return m.findResult, nil
}

func (m *mockSeriesFinder) setFindResult(result []Series) {
	m.findResult = result
}

type mockTargetGetter struct {
	getTargetsResults map[schema.MKey]models.Series
}

func (m *mockTargetGetter) getTargets(ctx context.Context, ss *models.StorageStats, reqs []models.Req) ([]models.Series, error) {
	results := make([]models.Series, 0, len(reqs))
	for _, req := range reqs {
		if targetResult, ok := m.getTargetsResults[req.MKey]; ok {
			results = append(results, targetResult)
		}
	}
	return results, nil
}

func (m *mockTargetGetter) setGetTargetsResults(results map[schema.MKey]models.Series) {
	m.getTargetsResults = results
}

func newMockTargetGetter() *mockTargetGetter {
	return &mockTargetGetter{}
}

func newMockApi() (*Server, *mockSeriesFinder, *mockTargetGetter) {
	seriesFinder := newMockSeriesFinder()
	targetGetter := newMockTargetGetter()
	return &Server{
		seriesFinder: seriesFinder,
		targetGetter: targetGetter,
	}, seriesFinder, targetGetter
}

func TestExecutePlanConcurrently(t *testing.T) {
	runCount := 100
	runConcurrently := 10
	concurrency := make(chan struct{}, runConcurrently)

	mdata.Aggregations = conf.NewAggregations()
	mdata.Schemas = conf.NewSchemas(nil)

	tracer, traceCloser, err := jaeger.Get()
	if err != nil {
		log.Fatalf("Could not initialize jaeger tracer: %s", err.Error())
	}
	defer traceCloser.Close()

	for i := 0; i < runCount; i++ {
		concurrency <- struct{}{}
		go func() {
			executePlanVerifyResult(t, tracer)
			<-concurrency
		}()
	}
}

func executePlanVerifyResult(t *testing.T, tracer opentracing.Tracer) {
	api, seriesFinder, targetGetter := newMockApi()

	span := tracer.StartSpan("test", nil)
	ctx := opentracing.ContextWithSpan(context.Background(), span)
	orgId := uint32(1)
	to := uint32(time.Now().UTC().Unix())
	from := to - 10
	queryPattern := "a.*.*"
	exprs, err := expr.ParseMany([]string{queryPattern})
	if err != nil {
		t.Fatalf("Unexpected error when parsing expression: %s", err)
	}

	plan, err := expr.NewPlan(exprs, from, to, 0, true, expr.Optimizations{})
	if err != nil {
		t.Fatalf("Unexpected error from expr.NewPlan(): %s", err)
	}

	metricCount := uint32(20000)
	findResult := []Series{
		{
			Pattern: queryPattern,
			Series:  getNodeSeries(metricCount, orgId, metricCount*0, "a.a."),
			Node: cluster.HTTPNode{
				Name:       "node1",
				Primary:    true,
				Partitions: []int32{1},
				Mode:       cluster.ModeShard,
				State:      cluster.NodeReady,
				Priority:   10,
			},
		},
		{
			Pattern: queryPattern,
			Series:  getNodeSeries(metricCount, orgId, metricCount*1, "a.b."),
			Node: cluster.HTTPNode{
				Name:       "node2",
				Primary:    true,
				Partitions: []int32{2},
				Mode:       cluster.ModeShard,
				State:      cluster.NodeReady,
				Priority:   10,
			},
		},
	}
	seriesFinder.setFindResult(findResult)

	getTargetsResult := make(map[schema.MKey]models.Series)
	var expectedResults []models.Series
	i := uint32(0)
	for _, nodeSeries := range findResult {
		for _, series := range nodeSeries.Series {
			res := models.Series{
				Target:     series.Defs[0].Name,
				QueryPatt:  "a.*.*",
				QueryFrom:  from,
				QueryTo:    to,
				Interval:   1,
				Datapoints: getDataPointsStartingAt(float64(i), from, to-from),
			}
			getTargetsResult[series.Defs[0].Id] = res
			expectedResults = append(expectedResults, res)
			i++
		}
	}

	targetGetter.setGetTargetsResults(getTargetsResult)

	response, _, err := api.executePlan(ctx, orgId, &plan)
	if err != nil {
		t.Fatalf("Error response from executePlan(): %s", err)
	}

	for i, expectedResult := range expectedResults {
		result := response[i]

		if result.Target != expectedResult.Target {
			t.Fatalf("Expected target %s but got %s", expectedResult.Target, result.Target)
		}
		if result.Interval != expectedResult.Interval {
			t.Fatalf("Expected interval %d but got %d", expectedResult.Interval, result.Interval)
		}

		for j, expectedDatapoint := range expectedResult.Datapoints {
			datapoint := result.Datapoints[j]

			if datapoint != expectedDatapoint {
				t.Fatalf("Expected data point %+v but got %+v", expectedDatapoint, datapoint)
			}
		}
	}
}

func getNodeSeries(metricCount, orgId, keyOffset uint32, prefix string) []idx.Node {
	nodeSeries := make([]idx.Node, 0, metricCount)

	for i := uint32(0); i < metricCount; i++ {
		metricName := fmt.Sprintf("%s%010d", prefix, i)
		nodeSeries = append(nodeSeries, idx.Node{
			Path: metricName,
			Leaf: true,
			Defs: []idx.Archive{
				{
					MetricDefinition: schema.MetricDefinition{
						Id:       getMKey(i+keyOffset, orgId),
						OrgId:    orgId,
						Name:     metricName,
						Interval: 1,
					},
				},
			},
		})
	}
	return nodeSeries
}

func getMKey(base, orgId uint32) schema.MKey {
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, base)

	return schema.MKey{
		Key: schema.Key([16]byte{bs[3], bs[2], bs[1], bs[0]}),
		Org: orgId,
	}
}

func getDataPointsStartingAt(val float64, ts, count uint32) []schema.Point {
	res := make([]schema.Point, 0, count)
	for i := uint32(0); i < count; i++ {
		res = append(res, schema.Point{
			Val: val,
			Ts:  ts,
		})
		val++
		ts++
	}
	return res
}
