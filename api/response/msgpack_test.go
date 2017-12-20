package response

import (
	"math"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"gopkg.in/raintank/schema.v1"
)

func BenchmarkHttpRespMsgpackEmptySeries(b *testing.B) {
	data := []models.Series{
		{
			Target:     "an.empty.series",
			Datapoints: make([]schema.Point, 0),
			Interval:   10,
		},
	}
	var resp *Msgpack
	for n := 0; n < b.N; n++ {
		resp = NewMsgpack(200, models.SeriesByTarget(data))
		resp.Body()
		resp.Close()
	}
}

func BenchmarkHttpRespMsgpackEmptySeriesNeedsEscaping(b *testing.B) {
	data := []models.Series{
		{
			Target:     `an.empty\series`,
			Datapoints: make([]schema.Point, 0),
			Interval:   10,
		},
	}
	var resp *Msgpack
	for n := 0; n < b.N; n++ {
		resp = NewMsgpack(200, models.SeriesByTarget(data))
		resp.Body()
		resp.Close()
	}
}

func BenchmarkHttpRespMsgpackIntegers(b *testing.B) {
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
	var resp *Msgpack
	for n := 0; n < b.N; n++ {
		resp = NewMsgpack(200, models.SeriesByTarget(data))
		resp.Body()
		resp.Close()
	}
}

func BenchmarkHttpRespMsgpackFloats(b *testing.B) {
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
	var resp *Msgpack
	for n := 0; n < b.N; n++ {
		resp = NewMsgpack(200, models.SeriesByTarget(data))
		resp.Body()
		resp.Close()
	}
}

func BenchmarkHttpRespMsgpackNulls(b *testing.B) {
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
	var resp *Msgpack
	for n := 0; n < b.N; n++ {
		resp = NewMsgpack(200, models.SeriesByTarget(data))
		resp.Body()
		resp.Close()
	}
}
