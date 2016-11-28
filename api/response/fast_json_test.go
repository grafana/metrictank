package response

import (
	"fmt"
	"math"
	"net/http/httptest"
	"testing"

	"github.com/raintank/metrictank/api/models"
	"gopkg.in/raintank/schema.v1"
)

func TestFastJson(t *testing.T) {
	for _, c := range testSeries() {
		w := httptest.NewRecorder()
		Write(w, NewFastJson(200, models.SeriesByTarget(c.in)))
		got := w.Body.String()
		if c.out != got {
			t.Fatalf("bad json output.\nexpected:%s\ngot:     %s\n", c.out, got)
		}
	}
}

func BenchmarkHttpRespFastJsonEmptySeries(b *testing.B) {
	data := []models.Series{
		{
			Target:     "an.empty.series",
			Datapoints: make([]schema.Point, 0),
			Interval:   10,
		},
	}
	var resp *FastJson
	for n := 0; n < b.N; n++ {
		resp = NewFastJson(200, models.SeriesByTarget(data))
		resp.Body()
		resp.Close()
	}
}

func BenchmarkHttpRespFastJsonEmptySeriesNeedsEscaping(b *testing.B) {
	data := []models.Series{
		{
			Target:     `an.empty\series`,
			Datapoints: make([]schema.Point, 0),
			Interval:   10,
		},
	}
	var resp *FastJson
	for n := 0; n < b.N; n++ {
		resp = NewFastJson(200, models.SeriesByTarget(data))
		resp.Body()
		resp.Close()
	}
}

func BenchmarkHttpRespFastJsonIntegers(b *testing.B) {
	points := make([]schema.Point, 1000, 1000)
	baseTs := 1500000000
	for i := 0; i < 1000; i++ {
		points[i] = schema.Point{Val: float64(10000 * i), Ts: uint32(baseTs + 10*i)}
	}
	data := []models.Series{
		{
			Target:     "some.metric.with.a-whole-bunch-of.integers",
			Datapoints: points,
			Interval:   10,
		},
	}
	b.SetBytes(int64(len(points) * 12))

	b.ResetTimer()
	var resp *FastJson
	for n := 0; n < b.N; n++ {
		resp = NewFastJson(200, models.SeriesByTarget(data))
		resp.Body()
		resp.Close()
	}
}
func BenchmarkHttpRespFastJsonFloats(b *testing.B) {
	points := make([]schema.Point, 1000, 1000)
	baseTs := 1500000000
	for i := 0; i < 1000; i++ {
		points[i] = schema.Point{Val: 12.34 * float64(i), Ts: uint32(baseTs + 10*i)}
	}
	data := []models.Series{
		{
			Target:     "some.metric.with.a-whole-bunch-of.floats",
			Datapoints: points,
			Interval:   10,
		},
	}
	b.SetBytes(int64(len(points) * 12))

	b.ResetTimer()
	var resp *FastJson
	for n := 0; n < b.N; n++ {
		resp = NewFastJson(200, models.SeriesByTarget(data))
		resp.Body()
		resp.Close()
	}
}
func BenchmarkHttpRespFastJsonNulls(b *testing.B) {
	points := make([]schema.Point, 1000, 1000)
	baseTs := 1500000000
	for i := 0; i < 1000; i++ {
		points[i] = schema.Point{Val: math.NaN(), Ts: uint32(baseTs + 10*i)}
	}
	data := []models.Series{
		{
			Target:     "some.metric.with.a-whole-bunch-of.nulls",
			Datapoints: points,
			Interval:   10,
		},
	}
	b.SetBytes(int64(len(points) * 12))

	b.ResetTimer()
	var resp *FastJson
	for n := 0; n < b.N; n++ {
		resp = NewFastJson(200, models.SeriesByTarget(data))
		resp.Body()
		resp.Close()
	}
}

func BenchmarkHttpRespFastJson1MMetricNames(b *testing.B) {
	series := make([]schema.MetricDefinition, 1000000)
	for i := 0; i < 1000000; i++ {
		series[i] = schema.MetricDefinition{
			Name: fmt.Sprintf("this.is.the.name.of.a.random-graphite-series.%d", i),
		}
	}
	b.ResetTimer()
	var resp *FastJson
	for n := 0; n < b.N; n++ {
		resp = NewFastJson(200, models.MetricNames(series))
		resp.Body()
		resp.Close()
	}
}

func BenchmarkHttpRespFastJson1MMetricNamesNeedEscaping(b *testing.B) {
	series := make([]schema.MetricDefinition, 1000000)
	for i := 0; i < 1000000; i++ {
		series[i] = schema.MetricDefinition{
			Name: fmt.Sprintf(`this.is.the.name.of.\.random\graphite\series.%d`, i),
		}
	}
	b.ResetTimer()
	var resp *FastJson
	for n := 0; n < b.N; n++ {
		resp = NewFastJson(200, models.MetricNames(series))
		resp.Body()
		resp.Close()
	}
}
