package mdata

import (
	"testing"
	"time"

	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/mdata/cache"
	"github.com/grafana/metrictank/test"
	"github.com/raintank/schema"
)

type testcase struct {
	ts       uint32
	span     uint32
	boundary uint32
}

func TestAggBoundary(t *testing.T) {
	cases := []testcase{
		{1, 120, 120},
		{50, 120, 120},
		{118, 120, 120},
		{119, 120, 120},
		{120, 120, 120},
		{121, 120, 240},
		{122, 120, 240},
		{150, 120, 240},
		{237, 120, 240},
		{238, 120, 240},
		{239, 120, 240},
		{240, 120, 240},
		{241, 120, 360},
	}
	for _, c := range cases {
		if ret := AggBoundary(c.ts, c.span); ret != c.boundary {
			t.Fatalf("aggBoundary for ts %d with span %d should be %d, not %d", c.ts, c.span, c.boundary, ret)
		}
	}
}

// note that values don't get "committed" to the metric until the aggregation interval is complete
func TestAggregator(t *testing.T) {
	cluster.Init("default", "test", time.Now(), "http", 6060)
	compare := func(key string, metric Metric, expected []schema.Point) {
		cluster.Manager.SetPrimary(true)
		res, err := metric.Get(0, 1000)

		if err != nil {
			t.Fatalf("expected err nil, got %v", err)
		}

		got := make([]schema.Point, 0, len(expected))
		for _, iter := range res.Iters {
			for iter.Next() {
				ts, val := iter.Values()
				got = append(got, schema.Point{Val: val, Ts: ts})
			}
		}
		if len(got) != len(expected) {
			t.Fatalf("output for testcase %s mismatch: expected: %v points, got: %v", key, len(expected), len(got))

		} else {
			for i, g := range got {
				exp := expected[i]
				if exp.Val != g.Val || exp.Ts != g.Ts {
					t.Fatalf("output for testcase %s mismatch at point %d: expected: %v, got: %v", key, i, exp, g)
				}
			}
		}
		cluster.Manager.SetPrimary(false)
	}
	ret := conf.NewRetentionMT(60, 86400, 120, 10, 0)
	aggs := conf.Aggregation{
		AggregationMethod: []conf.Method{conf.Avg, conf.Min, conf.Max, conf.Sum, conf.Lst},
	}

	agg := NewAggregator(mockstore, &cache.MockCache{}, test.GetAMKey(0), ret, aggs, false, 0)
	agg.Add(100, 123.4)
	agg.Add(110, 5)
	expected := []schema.Point{}
	compare("simple-min-unfinished", agg.minMetric, expected)

	agg = NewAggregator(mockstore, &cache.MockCache{}, test.GetAMKey(1), ret, aggs, false, 0)
	agg.Add(100, 123.4)
	agg.Add(110, 5)
	agg.Add(130, 130)
	expected = []schema.Point{
		{Val: 5, Ts: 120},
	}
	compare("simple-min-one-block", agg.minMetric, expected)

	// points with a timestamp belonging to the previous aggregation are ignored
	agg = NewAggregator(mockstore, &cache.MockCache{}, test.GetAMKey(1), ret, aggs, false, 0)
	agg.Add(100, 123.4)
	agg.Add(110, 5)
	agg.Add(130, 130)
	agg.Add(90, 24)
	expected = []schema.Point{
		{Val: 5, Ts: 120},
	}
	compare("simple-min-ignore-back-in-time", agg.minMetric, expected)

	// chunkspan is 120, ingestFrom = 140 means points before chunk starting at 240 are discarded
	agg = NewAggregator(mockstore, &cache.MockCache{}, test.GetAMKey(1), ret, aggs, false, 140)
	agg.Add(100, 123.4)
	agg.Add(110, 5)
	// this point is not flushed to agg.minMetric because no point after it with a timestamp
	// crossing the aggregation boundary is added (aggregation span here is 60)
	agg.Add(130, 130)
	expected = []schema.Point{}
	compare("simple-min-ingest-from-all-before-next-chunk", agg.minMetric, expected)

	// chunkspan is 120, ingestFrom = 115 means points before chunk starting at 120 are discarded
	agg = NewAggregator(mockstore, &cache.MockCache{}, test.GetAMKey(1), ret, aggs, false, 115)
	agg.Add(100, 123.4)
	agg.Add(110, 5)
	// this point is not flushed to agg.minMetric for the same reason as in the previous test
	agg.Add(130, 130)
	expected = []schema.Point{
		{Val: 5, Ts: 120},
	}
	compare("simple-min-ingest-from-one-in-next-chunk", agg.minMetric, expected)

	// chunkspan is 120, ingestFrom = 120 means points before chunk starting at 120 are discarded
	agg = NewAggregator(mockstore, &cache.MockCache{}, test.GetAMKey(1), ret, aggs, false, 120)
	agg.Add(100, 123.4)
	agg.Add(110, 5)
	// this point is not flushed to agg.minMetric for the same reason as in the previous test
	agg.Add(130, 130)
	expected = []schema.Point{
		{Val: 5, Ts: 120},
	}
	compare("simple-min-ingest-from-on-chunk-boundary", agg.minMetric, expected)

	// chunkspan is 120, ingestFrom = 170 means points before chunk starting at 240 are discarded
	// but, each aggregation contains 60s of data, so the point at 240 covers raw data from 181..240

	// raw data         :   1..120 121..180 181..240 241..300 301..360
	// discarded data   :   xxxxxxxxxxxxxxxx
	// aggregated points:      120      180      240      300      360
	// chunks by t0     :      120               240      300      360
	// discarded chunk  :   xxxxxxxxxxxxxxxxxxxxx
	agg = NewAggregator(mockstore, &cache.MockCache{}, test.GetAMKey(1), ret, aggs, false, 170)
	agg.Add(1, 1.1)
	agg.Add(119, 119)
	agg.Add(120, 120)
	agg.Add(121, 121)
	agg.Add(179, 179)
	agg.Add(180, 180)
	agg.Add(181, 181)
	agg.Add(220, 220)
	agg.Add(230, 230)
	agg.Add(231, 231)
	agg.Add(233, 233)
	agg.Add(239, 239)
	agg.Add(240, 240)
	agg.Add(245, 245)
	agg.Add(249, 249)
	agg.Add(250, 250)
	agg.Add(299, 299)
	agg.Add(300, 300)
	// these points are not flushed to agg.sumMetric for the same reason as in the previous test
	agg.Add(301, 0)
	agg.Add(320, 0)

	expected = []schema.Point{
		{Val: 181 + 220 + 230 + 231 + 233 + 239 + 240, Ts: 240},
		{Val: 245 + 249 + 250 + 299 + 300, Ts: 300},
	}
	compare("multi-sum-ingest-from-one-in-next-chunk", agg.sumMetric, expected)

	agg = NewAggregator(mockstore, &cache.MockCache{}, test.GetAMKey(2), ret, aggs, false, 0)
	agg.Add(100, 123.4)
	agg.Add(110, 5)
	agg.Add(120, 4)
	expected = []schema.Point{
		{Val: 4, Ts: 120},
	}
	compare("simple-min-one-block-done-cause-last-point-just-right", agg.minMetric, expected)

	agg = NewAggregator(mockstore, &cache.MockCache{}, test.GetAMKey(3), ret, aggs, false, 0)
	agg.Add(100, 123.4)
	agg.Add(110, 5)
	agg.Add(150, 1.123)
	agg.Add(180, 1)
	expected = []schema.Point{
		{Val: 5, Ts: 120},
		{Val: 1, Ts: 180},
	}
	compare("simple-min-two-blocks-done-cause-last-point-just-right", agg.minMetric, expected)

	agg = NewAggregator(mockstore, &cache.MockCache{}, test.GetAMKey(4), ret, aggs, false, 0)
	agg.Add(100, 123.4)
	agg.Add(110, 5)
	agg.Add(190, 2451.123)
	agg.Add(200, 1451.123)
	agg.Add(220, 978894.445)
	agg.Add(250, 1)
	compare("simple-min-skip-a-block", agg.minMetric, []schema.Point{
		{Val: 5, Ts: 120},
		{Val: 1451.123, Ts: 240},
	})
	compare("simple-max-skip-a-block", agg.maxMetric, []schema.Point{
		{Val: 123.4, Ts: 120},
		{Val: 978894.445, Ts: 240},
	})
	compare("simple-cnt-skip-a-block", agg.cntMetric, []schema.Point{
		{Val: 2, Ts: 120},
		{Val: 3, Ts: 240},
	})
	compare("simple-lst-skip-a-block", agg.lstMetric, []schema.Point{
		{Val: 5, Ts: 120},
		{Val: 978894.445, Ts: 240},
	})
	compare("simple-sum-skip-a-block", agg.sumMetric, []schema.Point{
		{Val: 128.4, Ts: 120},
		{Val: 2451.123 + 1451.123 + 978894.445, Ts: 240},
	})

}
