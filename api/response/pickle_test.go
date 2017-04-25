package response

import (
	"math"
	"testing"

	"github.com/raintank/metrictank/api/models"
	"gopkg.in/raintank/schema.v1"
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
		resp = NewPickle(200, models.SeriesPickleFormat(data))
		resp.Body()
		resp.Close()
	}
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
		resp = NewPickle(200, models.SeriesPickleFormat(data))
		resp.Body()
		resp.Close()
	}
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
		resp = NewPickle(200, models.SeriesPickleFormat(data))
		resp.Body()
		resp.Close()
	}
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
		resp = NewPickle(200, models.SeriesPickleFormat(data))
		resp.Body()
		resp.Close()
	}
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
		resp = NewPickle(200, models.SeriesPickleFormat(data))
		resp.Body()
		resp.Close()
	}
}

var foo []models.SeriesForPickle

func BenchmarkConvertSeriesToPickleFormat10k(b *testing.B) {
	points := make([]schema.Point, 10000)
	baseTs := 1500000000
	for i := 0; i < 10000; i++ {
		points[i] = schema.Point{Val: 1.2345 * float64(i), Ts: uint32(baseTs + 10*i)}
	}
	data := []models.Series{
		{
			Target:     "foo",
			Datapoints: points,
			Interval:   10,
		},
	}
	b.SetBytes(int64(len(points) * 12))

	b.ResetTimer()
	var bar []models.SeriesForPickle
	for n := 0; n < b.N; n++ {
		bar = models.SeriesPickleFormat(data)
	}
	foo = bar
}
