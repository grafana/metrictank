package expr

import (
	"math"
	"testing"

	"github.com/raintank/metrictank/api/models"
	"gopkg.in/raintank/schema.v1"
)

func TestSumSeriesIdentity(t *testing.T) {
	testSumSeries(
		"identity",
		[]models.Series{
			{
				Datapoints: []schema.Point{
					{Val: 0, Ts: 10},
					{Val: 10, Ts: 20},
					{Val: 100, Ts: 30},
					{Val: 5.5, Ts: 40},
					{Val: math.NaN(), Ts: 50},
					{Val: 1234567890, Ts: 60},
				},
			},
		},
		models.Series{
			Datapoints: []schema.Point{
				{Val: 0, Ts: 10},
				{Val: 10, Ts: 20},
				{Val: 100, Ts: 30},
				{Val: 5.5, Ts: 40},
				{Val: math.NaN(), Ts: 50},
				{Val: 1234567890, Ts: 60},
			},
		},
		t,
	)
}
func TestSumSeriesMultiple(t *testing.T) {
	testSumSeries(
		"sum-multiple",
		[]models.Series{
			{
				Datapoints: []schema.Point{
					{Val: 0, Ts: 10},
					{Val: 10, Ts: 20},
					{Val: 100, Ts: 30},
					{Val: 5.5, Ts: 40},
					{Val: math.NaN(), Ts: 50},
					{Val: math.NaN(), Ts: 60},
					{Val: 0, Ts: 70},
				},
			},
			{
				Datapoints: []schema.Point{
					{Val: 20, Ts: 10},
					{Val: 10, Ts: 20},
					{Val: 155.5, Ts: 30},
					{Val: 5.5, Ts: 40},
					{Val: math.NaN(), Ts: 50},
					{Val: 1234567890, Ts: 60},
					{Val: math.NaN(), Ts: 70},
				},
			},
		},
		models.Series{
			Datapoints: []schema.Point{
				{Val: 20, Ts: 10},
				{Val: 20, Ts: 20},
				{Val: 255.5, Ts: 30},
				{Val: 11, Ts: 40},
				{Val: math.NaN(), Ts: 50},
				{Val: math.NaN(), Ts: 60},
				{Val: math.NaN(), Ts: 70},
			},
		},
		t,
	)
}

func testSumSeries(name string, in []models.Series, out models.Series, t *testing.T) {
	f := NewSumSeries()
	got, err := f.Exec(make(map[Req][]models.Series), interface{}(in))
	if err != nil {
		t.Fatalf("case %q: err should be nil. got %q", name, err)
	}
	if len(got) != 1 {
		t.Fatalf("case %q: sumSeries output should be only 1 thing (a series) not %d", name, len(got))
	}
	o, ok := got[0].(models.Series)
	if !ok {
		t.Fatalf("case %q: expected sum output of models.Series type", name)
	}
	if len(o.Datapoints) != len(out.Datapoints) {
		t.Fatalf("case %q: len output expected %d, got %d", name, len(out.Datapoints), len(o.Datapoints))
	}
	for j, p := range o.Datapoints {
		bothNaN := math.IsNaN(p.Val) && math.IsNaN(out.Datapoints[j].Val)
		if (bothNaN || p.Val == out.Datapoints[j].Val) && p.Ts == out.Datapoints[j].Ts {
			continue
		}
		t.Fatalf("case %q: output point %d - expected %v got %v", name, j, out.Datapoints[j], p)
	}
}
