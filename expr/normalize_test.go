package expr

import (
	"fmt"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/consolidation"
	"github.com/grafana/metrictank/schema"
)

func TestNormalizeOneSeriesAdjustWithPreCanonicalize(t *testing.T) {
	in := []models.Series{
		{
			Interval:     5,
			Consolidator: consolidation.Sum,
			Datapoints: []schema.Point{
				// ts=5 null will be added to make pre-canonical wrt to 10
				{Ts: 10, Val: 10},
				{Ts: 15, Val: 15},
				{Ts: 20, Val: 20},
			},
		},
		{
			Interval:     10,
			Consolidator: consolidation.Sum,
			Datapoints: []schema.Point{
				{Ts: 10, Val: 10},
				{Ts: 20, Val: 20},
			},
		},
	}
	want := []models.Series{
		{
			Interval:     10,
			Consolidator: consolidation.Sum,
			Datapoints: []schema.Point{
				{Ts: 10, Val: 10},
				{Ts: 20, Val: 35},
			},
		},
		{
			Interval:     10,
			Consolidator: consolidation.Sum,
			Datapoints: []schema.Point{
				{Ts: 10, Val: 10},
				{Ts: 20, Val: 20},
			},
		},
	}
	cache := make(map[Req][]models.Series)
	got := Normalize(cache, in)
	fmt.Println("got:")
	fmt.Println(got[0].Datapoints)
	fmt.Println(got[1].Datapoints)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("TestNormalize() mismatch (-want +got):\n%s", diff)
	}
}

// LCM is 60, which means we need to generate points with ts=60 and ts=120
func TestNormalizeMultiLCMSeriesAdjustWithPreCanonicalize(t *testing.T) {
	in := []models.Series{
		{
			Interval:     15,
			Consolidator: consolidation.Sum,
			Datapoints: []schema.Point{
				{Ts: 15, Val: 15},
				{Ts: 30, Val: 30},
				{Ts: 45, Val: 45},
				{Ts: 60, Val: 60},
				{Ts: 75, Val: 75},
			},
		},
		{
			Interval:     20,
			Consolidator: consolidation.Sum,
			Datapoints: []schema.Point{
				{Ts: 20, Val: 20},
				{Ts: 40, Val: 40},
				{Ts: 60, Val: 60},
				{Ts: 80, Val: 80},
			},
		},
		{
			Interval:     10,
			Consolidator: consolidation.Sum,
			Datapoints: []schema.Point{
				// ts 10 will be added to make pre-canonical wrt to 60
				{Ts: 20, Val: 20},
				{Ts: 30, Val: 30},
				{Ts: 40, Val: 40},
				{Ts: 50, Val: 50},
				{Ts: 60, Val: 60},
				{Ts: 70, Val: 70},
				{Ts: 80, Val: 80},
			},
		},
	}
	want := []models.Series{
		{
			Interval:     60,
			Consolidator: consolidation.Sum,
			Datapoints: []schema.Point{
				{Ts: 60, Val: 150},
				{Ts: 120, Val: 75},
			},
		},
		{
			Interval:     60,
			Consolidator: consolidation.Sum,
			Datapoints: []schema.Point{
				{Ts: 60, Val: 120},
				{Ts: 120, Val: 80},
			},
		},
		{
			Interval:     60,
			Consolidator: consolidation.Sum,
			Datapoints: []schema.Point{
				{Ts: 60, Val: 200},
				{Ts: 120, Val: 150},
			},
		},
	}
	cache := make(map[Req][]models.Series)
	got := Normalize(cache, in)
	fmt.Println("got:")
	fmt.Println(got[0].Datapoints)
	fmt.Println(got[1].Datapoints)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("TestNormalize() mismatch (-want +got):\n%s", diff)
	}
}
