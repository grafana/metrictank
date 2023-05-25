package response

import (
	"math"
	"net/http/httptest"
	"testing"

	"github.com/grafana/metrictank/internal/schema"
	"github.com/grafana/metrictank/pkg/api/models"
)

func TestCsv(t *testing.T) {
	for _, c := range testSeries() {
		w := httptest.NewRecorder()
		Write(w, NewCsv(200, models.SeriesByTarget(c.in)))
		got := w.Body.String()
		if c.outCsv != got {
			t.Fatalf("bad csv output.\nexpected:%s\ngot:%s\n", c.outCsv, got)
		}
	}
}

func BenchmarkHttpRespCsvEmptySeries(b *testing.B) {
	data := []models.Series{
		{
			Target:     "an.empty.series",
			Datapoints: make([]schema.Point, 0),
			Interval:   10,
		},
	}
	var resp *Csv
	for n := 0; n < b.N; n++ {
		resp = NewCsv(200, models.SeriesByTarget(data))
		body, _ := resp.Body()
		size = len(body)
		resp.Close()
	}
	b.Log("body size", size)
}

func BenchmarkHttpRespCsvEmptySeriesNeedsEscaping(b *testing.B) {
	data := []models.Series{
		{
			Target:     `an.empty\series`,
			Datapoints: make([]schema.Point, 0),
			Interval:   10,
		},
	}
	var resp *Csv
	for n := 0; n < b.N; n++ {
		resp = NewCsv(200, models.SeriesByTarget(data))
		body, _ := resp.Body()
		size = len(body)
		resp.Close()
	}
	b.Log("body size", size)
}

func BenchmarkHttpRespCsvIntegers(b *testing.B) {
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
	var resp *Csv
	for n := 0; n < b.N; n++ {
		resp = NewCsv(200, models.SeriesByTarget(data))
		body, _ := resp.Body()
		size = len(body)
		resp.Close()
	}
	b.Log("body size", size)
}

func BenchmarkHttpRespCsvFloats(b *testing.B) {
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
	var resp *Csv
	for n := 0; n < b.N; n++ {
		resp = NewCsv(200, models.SeriesByTarget(data))
		body, _ := resp.Body()
		size = len(body)
		resp.Close()
	}
	b.Log("body size", size)
}

func BenchmarkHttpRespCsvNulls(b *testing.B) {
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
	var resp *Csv
	for n := 0; n < b.N; n++ {
		resp = NewCsv(200, models.SeriesByTarget(data))
		body, _ := resp.Body()
		size = len(body)
		resp.Close()
	}
	b.Log("body size", size)
}
