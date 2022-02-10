package expr

import (
	"fmt"
	"math"
	"strconv"
	"testing"
	"time"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/api/tz"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
)

func TestLinearRegression(t *testing.T) {
	in := []models.Series{{
		Target: "test.value",
		Tags: map[string]string{
			"test": "value",
		},
		QueryPatt: "test.value",
		Interval:  60,
		QueryFrom: 180,
		QueryTo:   480,
		Datapoints: []schema.Point{
			{
				Val: 3,
				Ts:  180,
			},
			{
				Val: math.NaN(),
				Ts:  240,
			},
			{
				Val: 5,
				Ts:  300,
			},
			{
				Val: 6,
				Ts:  360,
			},
			{
				Val: math.NaN(),
				Ts:  420,
			},
			{
				Val: 8,
				Ts:  480,
			},
		},
	}}

	expected := []models.Series{{
		Target: "linearRegression(test.value, 180, 480)",
		Tags: map[string]string{
			"test":              "value",
			"linearRegressions": "180, 480",
		},
		QueryPatt: "linearRegression(test.value, 180, 480)",
		Interval:  60,
		QueryFrom: 1200,
		QueryTo:   1500,
		Datapoints: []schema.Point{
			{
				Val: 20,
				Ts:  1200,
			},
			{
				Val: 21,
				Ts:  1260,
			},
			{
				Val: 22,
				Ts:  1320,
			},
			{
				Val: 23,
				Ts:  1380,
			},
			{
				Val: 24,
				Ts:  1440,
			},
			{
				Val: 25,
				Ts:  1500,
			},
		},
	}}

	testLinearRegression(t, "00:03 19700101", "00:08 19700101", 1200, 1500, in, expected)
}

func TestLinearRegressionRelative(t *testing.T) {
	now := uint32(time.Now().Unix())
	normalizedNow := now / 60 * 60 // normalize to prevent test fragility

	in := []models.Series{{
		Target: "test.value",
		Tags: map[string]string{
			"test": "value",
		},
		QueryPatt: "test.value",
		Interval:  60,
		QueryFrom: normalizedNow - 1320,
		QueryTo:   normalizedNow - 1020,
		Datapoints: []schema.Point{
			{
				Val: 3,
				Ts:  normalizedNow - 1320,
			},
			{
				Val: math.NaN(),
				Ts:  normalizedNow - 1260,
			},
			{
				Val: 5,
				Ts:  normalizedNow - 1200,
			},
			{
				Val: 6,
				Ts:  normalizedNow - 1140,
			},
			{
				Val: math.NaN(),
				Ts:  normalizedNow - 1080,
			},
			{
				Val: 8,
				Ts:  normalizedNow - 1020,
			},
		},
	}}

	expected := []models.Series{{
		Target: fmt.Sprintf("linearRegression(test.value, %d, %d)", now-1320, now-1020),
		Tags: map[string]string{
			"test":              "value",
			"linearRegressions": fmt.Sprintf("%d, %d", now-1320, now-1020),
		},
		QueryPatt: fmt.Sprintf("linearRegression(test.value, %d, %d)", now-1320, now-1020),
		Interval:  60,
		QueryFrom: normalizedNow - 300,
		QueryTo:   normalizedNow,
		Datapoints: []schema.Point{
			{
				Val: 20,
				Ts:  normalizedNow - 300,
			},
			{
				Val: 21,
				Ts:  normalizedNow - 240,
			},
			{
				Val: 22,
				Ts:  normalizedNow - 180,
			},
			{
				Val: 23,
				Ts:  normalizedNow - 120,
			},
			{
				Val: 24,
				Ts:  normalizedNow - 60,
			},
			{
				Val: 25,
				Ts:  normalizedNow,
			},
		},
	}}

	testLinearRegression(t, "now-1320s", "now-1020s", normalizedNow-300, normalizedNow, in, expected)
}

func TestLinearRegressionNormalization(t *testing.T) {
	in := []models.Series{{
		Target: "test.value",
		Tags: map[string]string{
			"test": "value",
		},
		QueryPatt: "test.value",
		Interval:  60,
		QueryFrom: 180,
		QueryTo:   480,
		Datapoints: []schema.Point{
			{
				Val: 3,
				Ts:  180,
			},
			{
				Val: math.NaN(),
				Ts:  240,
			},
			{
				Val: 5,
				Ts:  300,
			},
			{
				Val: 6,
				Ts:  360,
			},
			{
				Val: math.NaN(),
				Ts:  420,
			},
			{
				Val: 8,
				Ts:  480,
			},
		},
	}}

	expected := []models.Series{{
		Target: "linearRegression(test.value, 180, 480)",
		Tags: map[string]string{
			"test":              "value",
			"linearRegressions": "180, 480",
		},
		QueryPatt: "linearRegression(test.value, 180, 480)",
		Interval:  60,
		QueryFrom: 1199,
		QueryTo:   1501,
		Datapoints: []schema.Point{
			{
				Val: 20,
				Ts:  1200,
			},
			{
				Val: 21,
				Ts:  1260,
			},
			{
				Val: 22,
				Ts:  1320,
			},
			{
				Val: 23,
				Ts:  1380,
			},
			{
				Val: 24,
				Ts:  1440,
			},
			{
				Val: 25,
				Ts:  1500,
			},
		},
	}}

	testLinearRegression(t, "00:03 19700101", "00:08 19700101", 1199, 1501, in, expected)
}

func testLinearRegression(t *testing.T, startSourceAt string, endSourceAt string, startTargetAt uint32, endTargetAt uint32, input []models.Series, expected []models.Series) {
	var err error
	tz.TimeZone, err = time.LoadLocation("")
	if err != nil {
		t.Fatalf("%s", err)
	}

	inputCopy := models.SeriesCopy(input) // to later verify that it is unchanged

	funcLinearRegression := FuncLinearRegression{
		in:            NewMock(input),
		startSourceAt: startSourceAt,
		endSourceAt:   endSourceAt,
	}

	context := Context{
		from: startTargetAt,
		to:   endTargetAt,
	}
	newContext := funcLinearRegression.Context(context)
	t.Run("ModifiedContext", func(t *testing.T) {
		if newContext.from == context.from || newContext.to == context.to {
			t.Fatal("the context is expected to be modified by the linear regression function")
		}
	})

	actual, err := funcLinearRegression.Exec(initDataMap(input))
	if err != nil {
		t.Fatal(err)
	}
	if err := equalOutput(expected, actual, nil, err); err != nil {
		t.Fatal(err)
	}

	t.Run("DidNotModifyInput", func(t *testing.T) {
		if err := equalOutput(inputCopy, input, nil, nil); err != nil {
			t.Fatal("Input was modified: ", err)
		}
	})
}

func BenchmarkLinearRegression10k_1NoNulls(b *testing.B) {
	benchmarkLinearRegression(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkLinearRegression10k_10NoNulls(b *testing.B) {
	benchmarkLinearRegression(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkLinearRegression10k_100NoNulls(b *testing.B) {
	benchmarkLinearRegression(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkLinearRegression10k_1000NoNulls(b *testing.B) {
	benchmarkLinearRegression(b, 1000, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkLinearRegression10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkLinearRegression(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkLinearRegression10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkLinearRegression(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkLinearRegression10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkLinearRegression(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkLinearRegression10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkLinearRegression(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkLinearRegression10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkLinearRegression(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkLinearRegression10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkLinearRegression(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkLinearRegression10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkLinearRegression(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkLinearRegression10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkLinearRegression(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func benchmarkLinearRegression(b *testing.B, numSeries int, fn0, fn1 test.DataFunc) {
	var err error
	tz.TimeZone, err = time.LoadLocation("")
	if err != nil {
		b.Fatalf("%s", err)
	}

	var input []models.Series
	for i := 0; i < numSeries; i++ {
		series := models.Series{
			QueryPatt: strconv.Itoa(i),
		}
		if i%2 == 0 {
			series.Datapoints, series.Interval = fn0()
		} else {
			series.Datapoints, series.Interval = fn1()
		}
		input = append(input, series)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := NewLinearRegression()
		f.(*FuncLinearRegression).in = NewMock(input)
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
