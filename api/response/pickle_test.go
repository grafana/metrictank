package response

import (
	"math"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/raintank/schema"
)

func BenchmarkHttpRespPickleEmptySeries(b *testing.B) {
	data := []models.Series{
		{
			Target:     "an.empty.series",
			Datapoints: make([]schema.Point, 0),
			Interval:   10,
		},
	}
	var resp *Pickle
	for n := 0; n < b.N; n++ {
		resp = NewPickle(200, models.SeriesByTarget(data))
		resp.Body()
		body, _ := resp.Body()
		size = len(body)
		resp.Close()
	}
	b.Log("body size", size)
}

func BenchmarkHttpRespPickleEmptySeriesNeedsEscaping(b *testing.B) {
	data := []models.Series{
		{
			Target:     `an.empty\series`,
			Datapoints: make([]schema.Point, 0),
			Interval:   10,
		},
	}
	var resp *Pickle
	for n := 0; n < b.N; n++ {
		resp = NewPickle(200, models.SeriesByTarget(data))
		resp.Body()
		body, _ := resp.Body()
		size = len(body)
		resp.Close()
	}
	b.Log("body size", size)
}

func BenchmarkHttpRespPickleIntegers(b *testing.B) {
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
	var resp *Pickle
	for n := 0; n < b.N; n++ {
		resp = NewPickle(200, models.SeriesByTarget(data))
		resp.Body()
		body, _ := resp.Body()
		size = len(body)
		resp.Close()
	}
	b.Log("body size", size)
}

func BenchmarkHttpRespPickleFloats(b *testing.B) {
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
	var resp *Pickle
	for n := 0; n < b.N; n++ {
		resp = NewPickle(200, models.SeriesByTarget(data))
		resp.Body()
		body, _ := resp.Body()
		size = len(body)
		resp.Close()
	}
	b.Log("body size", size)
}

func BenchmarkHttpRespPickleNulls(b *testing.B) {
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
	var resp *Pickle
	for n := 0; n < b.N; n++ {
		resp = NewPickle(200, models.SeriesByTarget(data))
		resp.Body()
		body, _ := resp.Body()
		size = len(body)
		resp.Close()
	}
	b.Log("body size", size)
}
