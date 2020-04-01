package expr

import (
	"math"
	"sort"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/errors"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
)

func TestNoNodes(t *testing.T) {
	in := []models.Series{
		getModel("name1;tag1=val1", a),
	}
	expected := errors.NewBadRequest("No nodes specified")

	testGroupByNodes("ErrNoTags", in, nil, "sum", []expr{}, expected, t)
}

func TestGroupByNodesSingleSeries(t *testing.T) {
	in := []models.Series{
		getModel("name.node1;tag=val", a),
	}
	expectedByName := []models.Series{
		getModel("node1", a),
	}
	expectedByTag := []models.Series{
		getModel("val", a),
	}

	aggs := []string{
		"avg",
		"average",
		"median",
		"sum",
		"min",
		"max",
		"stddev",
		"diff",
		"range",
		"multiply",
	}

	for _, agg := range aggs {
		expectedByName[0].Datapoints = expectedByName[0].Datapoints[:0]
		expectedByTag[0].Datapoints = expectedByTag[0].Datapoints[:0]
		aggFunc := getCrossSeriesAggFunc(agg)
		aggFunc(in, &expectedByName[0].Datapoints)
		aggFunc(in, &expectedByTag[0].Datapoints)
		testGroupByNodes(
			"SingleSeriesByName("+agg+")",
			in,
			expectedByName,
			agg,
			[]expr{{etype: etInt, str: "1", int: 1}},
			nil,
			t)
		testGroupByNodes(
			"SingleSeriesByTag("+agg+")",
			in,
			expectedByTag,
			agg,
			[]expr{{etype: etString, str: "tag"}},
			nil,
			t)
	}
}

func TestGroupByNodesMultipleSeriesSingleResult(t *testing.T) {
	in := []models.Series{
		getModel("name.node1.node2;tag1=val1;tag2=val2", a),
		getModel("name.node1.node2;tag1=val1;tag3=val3", b),
	}
	expectedByName := []models.Series{
		getModel("node1", sumab),
	}
	expectedByTag := []models.Series{
		getModel("val1", sumab),
	}
	expectedByNameAndTag := []models.Series{
		getModel("node1.val1", sumab),
	}
	testGroupByNodes(
		"MultipleSeriesSingleResultByName",
		in,
		expectedByName,
		"sum",
		[]expr{{etype: etInt, str: "1", int: 1}}, nil, t)
	testGroupByNodes(
		"MultipleSeriesSingleResultByTag",
		in,
		expectedByTag,
		"sum",
		[]expr{{etype: etString, str: "tag1"}},
		nil,
		t)
	testGroupByNodes(
		"MultipleSeriesSingleResultByNameAndTag",
		in,
		expectedByNameAndTag,
		"sum",
		[]expr{{etype: etInt, str: "1", int: 1}, {etype: etString, str: "tag1"}},
		nil,
		t)
}

func TestGroupByNodesMultipleSeriesMultipleResults(t *testing.T) {
	in := []models.Series{
		getModel("name.node1.node2;tag1=val1;tag2=val2", a),
		getModel("name.node1.node3;tag1=val1;tag3=val3", b),
		getModel("name.node2.node3;tag1=val1_0;tag2=val2", c),
		getModel("name.node2.node4;tag1=val1_0;tag2=val2", d),
		getModel("name.node1.node2;tag1=val1_1;tag4=val4", c),
	}
	expected := []models.Series{
		getModel("node1.val1", sumab),
		getModel("node2.val1_0", sumcd),
		getModel("node1.val1_1", c),
	}
	testGroupByNodes(
		"MultipleSeriesMultipleResults",
		in,
		expected,
		"sum",
		[]expr{{etype: etInt, str: "1", int: 1}, {etype: etString, str: "tag1"}},
		nil,
		t)
}

func TestGroupByNodesMultipleSeriesGroupByName(t *testing.T) {
	in := []models.Series{
		getModel("name.node1.node2;tag1=val1;tag2=val2", c),
		getModel("name.node1.node2;tag1=val1;tag3=val3", d),
		getModel("name.node2.node3;tag1=val1_0;tag2=val2", a),
		getModel("name.node2.node3;tag1=val1_0;tag2=val2", b),
		getModel("name.node2.node3;tag1=val1_1;tag4=val4", c),
	}
	expected := []models.Series{
		getModel("name.node1.node2", sumcd),
		getModel("name.node2.node3", sumabc),
	}
	testGroupByNodes(
		"MultipleSeriesGroupByName",
		in,
		expected,
		"sum",
		[]expr{{etype: etString, str: "name"}},
		nil,
		t)
}

func TestGroupByNodesMultipleSeriesMissingNameNode(t *testing.T) {
	in := []models.Series{
		getModel("name.node1.node2;tag1=val1;tag2=val2", a),
		getModel("name.node1.node2;tag1=val1;tag3=val3", b),
	}
	expected := []models.Series{
		getModel("val1", sumab),
	}
	testGroupByNodes(
		"MultipleSeriesMissingNameNode",
		in,
		expected,
		"sum",
		[]expr{{etype: etInt, str: "1", int: 3}, {etype: etString, str: "tag1"}},
		nil,
		t)
}

func TestGroupByNodesMultipleSeriesMissingTag(t *testing.T) {
	in := []models.Series{
		getModel("name.node1.node2;tag1=val1;tag2=val2", a),
		getModel("name.node1.node2;tag1=val1;tag3=val3", b),
		getModel("name.node2.node3;tag2=val2_0;tag3=val3", c),
	}
	expected := []models.Series{
		getModel("node1.val1", sumab),
		getModel("node2.", c),
	}
	testGroupByNodes(
		"MultipleSeriesMissingTag",
		in,
		expected,
		"sum",
		[]expr{{etype: etInt, str: "1", int: 1}, {etype: etString, str: "tag1"}},
		nil,
		t)
}

func TestGroupByNodesAllAggregators(t *testing.T) {
	aggregators := []struct {
		name                      string
		result1, result2, result3 []schema.Point
	}{
		{name: "sum", result1: sumab, result2: sumabc, result3: sumab},
		{name: "avg", result1: avgab, result2: avgabc, result3: avgab},
		{name: "average", result1: avgab, result2: avgabc, result3: avgab},
		{name: "max", result1: maxab, result2: maxabc, result3: maxab},
		{name: "median", result1: medianab, result2: medianabc, result3: medianab},
		{name: "multiply", result1: multab, result2: multabc, result3: multab},
		{name: "stddev", result1: stddevab, result2: stddevabc, result3: stddevab},
		{name: "diff", result1: diffab, result2: diffabc, result3: diffab},
		{name: "range", result1: rangeab, result2: rangeabc, result3: rangeab},
	}

	for _, agg := range aggregators {
		in := []models.Series{
			getModel("name.node1;tag1=val1;tag2=val2_0", a),
			getModel("name.node1;tag1=val1;tag2=val2_1", b),
			getModel("name.node2;tag1=val1_1;tag2=val2_0", a),
			getModel("name.node2;tag1=val1_1;tag2=val2_1", b),
			getModel("name.node2;tag1=val1_1;tag2=val2_2", c),
			getModel("name.node3;tag1=val1_0;tag2=val2_3", a),
			getModel("name.node3;tag1=val1_0;tag2=val2_3", b),
		}
		expected := []models.Series{
			getModel("node1.val1", agg.result1),
			getModel("node2.val1_1", agg.result2),
			getModel("node3.val1_0", agg.result3),
		}

		testGroupByNodes("AllAggregators ("+agg.name+")",
			in,
			expected,
			agg.name,
			[]expr{{etype: etInt, str: "1", int: 1}, {etype: etString, str: "tag1"}},
			nil,
			t)
	}
}

func testGroupByNodes(name string, in []models.Series, expected []models.Series, aggr string, nodes []expr, expErr error, t *testing.T) {
	f := NewGroupByNodesConstructor(true)() 
    f.(*FuncGroupByNodes).in = NewMock(in)
    f.(*FuncGroupByNodes).aggregator = aggr
    f.(*FuncGroupByNodes).nodes = nodes
    got, err := f.Exec(make(map[Req][]models.Series))
	if err != expErr {
		if expErr == nil {
			t.Fatalf("case %q: expected no error but got %q", name, err)
		} else if err == nil || err.Error() != expErr.Error() {
			t.Fatalf("case %q: expected error %q but got %q", name, expErr, err)
		}
	}

	if len(got) != len(expected) {
		t.Fatalf("case %q: output length expected to be %d but got %d", name, len(expected), len(got))
	}

	sort.Slice(got, func(i, j int) bool {
		return got[i].Target < got[j].Target
	})
	sort.Slice(expected, func(i, j int) bool {
		return expected[i].Target < expected[j].Target
	})

	for i, g := range got {
		o := expected[i]
		if g.Target != o.Target {
			t.Fatalf("case %q: expected target %q, but got %q", name, o.Target, g.Target)
		}
		if len(g.Datapoints) != len(o.Datapoints) {
			t.Fatalf("case %q: expected output length %d, but got %d", name, len(o.Datapoints), len(g.Datapoints))
		}
		for j, p := range g.Datapoints {
			bothNaN := math.IsNaN(p.Val) && math.IsNaN(o.Datapoints[j].Val)
			if (bothNaN || p.Val == o.Datapoints[j].Val) && p.Ts == o.Datapoints[j].Ts {
				continue
			}
			t.Fatalf("case %q: output point %d - expected %v got %v", name, j, o.Datapoints[j], p)
		}
		if len(g.Tags) != len(o.Tags) {
			t.Fatalf("case %q: len tags expected %d, got %d", name, len(o.Tags), len(g.Tags))
		}
		for k, v := range g.Tags {
			expectedVal, ok := o.Tags[k]
			if !ok {
				t.Fatalf("case %q: Got unknown tag key '%s'", name, k)
			}

			if v != expectedVal {
				t.Fatalf("case %q: Key '%s' had wrong value: expected '%s', got '%s'", name, k, expectedVal, v)
			}
		}
	}
}

func BenchmarkGroupByNodes1in1out(b *testing.B) {
	benchmarkGroupByNodes(b, 1, 1)
}

func BenchmarkGroupByNodes10in1out(b *testing.B) {
	benchmarkGroupByNodes(b, 10, 1)
}

func BenchmarkGroupByNodes10in10out(b *testing.B) {
	benchmarkGroupByNodes(b, 1, 10)
}

func BenchmarkGroupByNodes100in1out(b *testing.B) {
	benchmarkGroupByNodes(b, 100, 1)
}

func BenchmarkGroupByNodes100in100out(b *testing.B) {
	benchmarkGroupByNodes(b, 10, 10)
}

func BenchmarkGroupByNodes1000in1out(b *testing.B) {
	benchmarkGroupByNodes(b, 1000, 1)
}

func BenchmarkGroupByNodes1000in100out(b *testing.B) {
	benchmarkGroupByNodes(b, 100, 10)
}

func BenchmarkGroupByNodes1000in1000out(b *testing.B) {
	benchmarkGroupByNodes(b, 1, 1000)
}

func BenchmarkGroupByNodes10000in1out(b *testing.B) {
	benchmarkGroupByNodes(b, 10000, 1)
}

func BenchmarkGroupByNodes10000in100out(b *testing.B) {
	benchmarkGroupByNodes(b, 1000, 10)
}

func BenchmarkGroupByNodes10000in10000out1(b *testing.B) {
	benchmarkGroupByNodes(b, 100, 100)
}

func BenchmarkGroupByNodes10000in10000out2(b *testing.B) {
	benchmarkGroupByNodes(b, 10, 1000)
}

func BenchmarkGroupByNodes100000in1out(b *testing.B) {
	benchmarkGroupByNodes(b, 100000, 1)
}

func BenchmarkGroupByNodes100000in100out(b *testing.B) {
	benchmarkGroupByNodes(b, 10000, 10)
}

func BenchmarkGroupByNodes100000in10000out(b *testing.B) {
	benchmarkGroupByNodes(b, 1000, 100)
}

// numIn is the number if series with different names,
// and numOut is the number of different tags of a series
func benchmarkGroupByNodes(b *testing.B, numIn, numOut int) {
	var input []models.Series
	nameValues := []string{"nameOne", "nameTwo"}
	tagValues := []string{"tag1", "tag2"}

	for i := 0; i < numIn; i++ {
		series := models.Series{
			Target:   strconv.Itoa(i),
			Interval: 10,
		}
		for _, nameNode := range nameValues {
			series.Target += "." + nameNode + strconv.Itoa(i%numOut)
		}
		for j := 0; j < numOut; j++ {
			for _, tag := range tagValues {
				series.Target += ";" + tag + "=" + strconv.Itoa(j%numOut)
			}
			series.Datapoints = test.RandFloats100()
			input = append(input, series)
		}
	}
	b.ResetTimer()
	var err error
	for i := 0; i < b.N; i++ {
		groupByNodes := NewGroupByNodesConstructor(true)()
		f := groupByNodes.(*FuncGroupByNodes)
		f.in = NewMock(input)
		f.aggregator = "sum"
		f.nodes = []expr{{etype: etInt, str: "1", int: 1}, {etype: etString, str: "tag1"}}

		results, err = f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}

		iters := numIn
		if numIn >= numOut {
			iters = numOut
		}
		expectedOut := iters * numOut
		if len(results) != expectedOut {
			b.Fatalf("Expected %d groups, got %d", expectedOut, len(results))
		}

		for _, serie := range results {
			pointSlicePool.Put(serie.Datapoints[:0])
		}
	}

    totalIn := numIn * numOut
	b.SetBytes(int64(totalIn * len(results[0].Datapoints) * 12))
}
