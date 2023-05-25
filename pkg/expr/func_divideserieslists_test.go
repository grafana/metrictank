package expr

import (
	"math"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/internal/schema"
	"github.com/grafana/metrictank/pkg/api/models"
	"github.com/grafana/metrictank/pkg/test"
)

func TestDivideSeriesListsSingle(t *testing.T) {
	testDivideSeriesLists(
		"single",
		[]models.Series{
			getSeries("foo;a=a;b=b", `seriesByTag("name=foo")`, []schema.Point{
				{Val: 0, Ts: 10},
				{Val: math.NaN(), Ts: 20},
				{Val: 0, Ts: 30},
				{Val: 1, Ts: 40},
			}),
		},
		[]models.Series{
			getSeries("bar;a=a1;b=b", `seriesByTag("name=bar")`, []schema.Point{
				{Val: 1, Ts: 10},
				{Val: 1, Ts: 20},
				{Val: 0, Ts: 30},
				{Val: 0, Ts: 40},
			}),
		},
		[]models.Series{
			{
				Interval:  10,
				QueryFrom: 10,
				QueryTo:   41,
				Target:    "divideSeries(foo;a=a;b=b,bar;a=a1;b=b)",
				QueryPatt: "divideSeries(foo;a=a;b=b,bar;a=a1;b=b)",
				Datapoints: []schema.Point{
					{Val: 0, Ts: 10},
					{Val: math.NaN(), Ts: 20},
					{Val: math.NaN(), Ts: 30},
					{Val: math.NaN(), Ts: 40},
				},
				Tags: map[string]string{
					"name": "divideSeries(foo;a=a;b=b,bar;a=a1;b=b)",
				},
			},
		},
		t,
	)
}

func TestDivideSeriesListsMultiple(t *testing.T) {
	testDivideSeriesLists(
		"multiple",
		[]models.Series{
			getSeries("foo-1;a=1;b=2;c=3", "foo-1", []schema.Point{
				{Val: 0, Ts: 10},
				{Val: math.NaN(), Ts: 20},
			}),
			getSeries("foo-2;a=2;b=2;b=2", "foo-2", []schema.Point{
				{Val: 20, Ts: 10},
				{Val: 100, Ts: 20},
			}),
		},
		[]models.Series{
			getSeries("overbar;a=3;b=2;c=1", "overbar", []schema.Point{
				{Val: 2, Ts: 10},
				{Val: math.NaN(), Ts: 20},
			}),
			getSeries("overbar-2;a=3;b=2;c=1", "overbar-2", []schema.Point{
				{Val: 1, Ts: 10},
				{Val: 2, Ts: 20},
			}),
		},
		[]models.Series{
			{
				Interval:  10,
				QueryFrom: 10,
				QueryTo:   21,
				Target:    "divideSeries(foo-1;a=1;b=2;c=3,overbar;a=3;b=2;c=1)",
				QueryPatt: "divideSeries(foo-1;a=1;b=2;c=3,overbar;a=3;b=2;c=1)",
				Datapoints: []schema.Point{
					{Val: 0, Ts: 10},
					{Val: math.NaN(), Ts: 20},
				},
				Tags: map[string]string{
					"name": "divideSeries(foo-1;a=1;b=2;c=3,overbar;a=3;b=2;c=1)",
				},
			},
			{
				Interval:  10,
				QueryFrom: 10,
				QueryTo:   21,
				Target:    "divideSeries(foo-2;a=2;b=2;b=2,overbar-2;a=3;b=2;c=1)",
				QueryPatt: "divideSeries(foo-2;a=2;b=2;b=2,overbar-2;a=3;b=2;c=1)",
				Datapoints: []schema.Point{
					{Val: 20, Ts: 10},
					{Val: 50, Ts: 20},
				},
				Tags: map[string]string{
					"name": "divideSeries(foo-2;a=2;b=2;b=2,overbar-2;a=3;b=2;c=1)",
				},
			},
		},
		t,
	)
}

func testDivideSeriesLists(name string, dividend, divisor []models.Series, out []models.Series, t *testing.T) {
	f := NewDivideSeriesLists()
	DivideSeriesLists := f.(*FuncDivideSeriesLists)
	DivideSeriesLists.dividends = NewMock(dividend)
	DivideSeriesLists.divisors = NewMock(divisor)

	dividendCopy := models.SeriesCopy(dividend) // to later verify that it is unchanged
	divisorCopy := models.SeriesCopy(divisor)   // to later verify that it is unchanged

	dataMap := initDataMapMultiple([][]models.Series{dividend, divisor})

	got, err := f.Exec(dataMap)
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatalf("Case %s: %s", name, err)
	}
	if err := equalTags(out, got); err != nil {
		t.Fatalf("Case %s: %s", name, err)
	}

	t.Run("DidNotModifyInput", func(t *testing.T) {
		if err := equalOutput(dividendCopy, dividend, nil, nil); err != nil {
			t.Fatalf("Case %s: Input was modified, err = %s", name, err)
		}
		if err := equalOutput(divisorCopy, divisor, nil, nil); err != nil {
			t.Fatalf("Case %s: Input was modified, err = %s", name, err)
		}
	})

	t.Run("DoesNotDoubleReturnPoints", func(t *testing.T) {
		if err := dataMap.CheckForOverlappingPoints(); err != nil {
			t.Fatalf("Case %s: Point slices in datamap overlap, err = %s", name, err)
		}
	})
	t.Run("OutputIsCanonical", func(t *testing.T) {
		for i, s := range got {
			if !s.IsCanonical() {
				t.Fatalf("Case %s: output series %d is not canonical: %v", name, i, s)
			}
		}
	})
}

func BenchmarkDivideSeriesLists10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkDivideSeriesLists(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkDivideSeriesLists10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkDivideSeriesLists(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkDivideSeriesLists10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkDivideSeriesLists(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkDivideSeriesLists10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkDivideSeriesLists(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}

func benchmarkDivideSeriesLists(b *testing.B, numSeries int, fn0, fn1 test.DataFunc) {
	var dividends []models.Series
	for i := 0; i < numSeries; i++ {
		series := models.Series{
			Target: strconv.Itoa(i),
		}
		if i%1 == 0 {
			series.Datapoints, series.Interval = fn0()
		} else {
			series.Datapoints, series.Interval = fn1()
		}
		dividends = append(dividends, series)
	}
	var divisors []models.Series
	for i := 0; i < numSeries; i++ {
		series := models.Series{
			Target: strconv.Itoa(i) + "-divisor",
		}
		if i%1 == 0 {
			series.Datapoints, series.Interval = fn0()
		} else {
			series.Datapoints, series.Interval = fn1()
		}
		divisors = append(divisors, series)
	}
	b.ResetTimer()
	var err error
	for i := 0; i < b.N; i++ {
		f := NewDivideSeriesLists()
		DivideSeriesLists := f.(*FuncDivideSeriesLists)
		DivideSeriesLists.dividends = NewMock(dividends)
		DivideSeriesLists.divisors = NewMock(divisors)
		results, err = f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
	}
	b.SetBytes(int64(numSeries * len(results[0].Datapoints) * 12 * 2))
}
