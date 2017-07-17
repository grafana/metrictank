package mdata

import (
	"testing"
	"time"

	"github.com/raintank/metrictank/cluster"
	"github.com/raintank/metrictank/conf"
	"github.com/raintank/metrictank/mdata/cache"
	"gopkg.in/raintank/schema.v1"
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
		if ret := aggBoundary(c.ts, c.span); ret != c.boundary {
			t.Fatalf("aggBoundary for ts %d with span %d should be %d, not %d", c.ts, c.span, c.boundary, ret)
		}
	}
}

// note that values don't get "committed" to the metric until the aggregation interval is complete
func TestAggregator(t *testing.T) {
	cluster.Init("default", "test", time.Now(), "http", 6060)
	compare := func(key string, metric Metric, expected []schema.Point) {
		cluster.Manager.SetPrimary(true)
		res := metric.Get(0, 1000)
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
	ret := conf.NewRetentionMT(60, 86400, 120, 10, true)
	aggs := conf.Aggregation{
		AggregationMethod: []conf.Method{conf.Avg, conf.Min, conf.Max, conf.Sum, conf.Lst},
	}

	agg := NewAggregator(dnstore, &cache.MockCache{}, "test", ret, aggs, false)
	agg.Add(100, 123.4)
	agg.Add(110, 5)
	expected := []schema.Point{}
	compare("simple-min-unfinished", agg.minMetric, expected)

	agg = NewAggregator(dnstore, &cache.MockCache{}, "test", ret, aggs, false)
	agg.Add(100, 123.4)
	agg.Add(110, 5)
	agg.Add(130, 130)
	expected = []schema.Point{
		{Val: 5, Ts: 120},
	}
	compare("simple-min-one-block", agg.minMetric, expected)

	agg = NewAggregator(dnstore, &cache.MockCache{}, "test", ret, aggs, false)
	agg.Add(100, 123.4)
	agg.Add(110, 5)
	agg.Add(120, 4)
	expected = []schema.Point{
		{Val: 4, Ts: 120},
	}
	compare("simple-min-one-block-done-cause-last-point-just-right", agg.minMetric, expected)

	agg = NewAggregator(dnstore, &cache.MockCache{}, "test", ret, aggs, false)
	agg.Add(100, 123.4)
	agg.Add(110, 5)
	agg.Add(150, 1.123)
	agg.Add(180, 1)
	expected = []schema.Point{
		{Val: 5, Ts: 120},
		{Val: 1, Ts: 180},
	}
	compare("simple-min-two-blocks-done-cause-last-point-just-right", agg.minMetric, expected)

	agg = NewAggregator(dnstore, &cache.MockCache{}, "test", ret, aggs, false)
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
