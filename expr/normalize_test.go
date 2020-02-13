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
			QueryTo:      21,
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
			QueryTo:      21,
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
			QueryTo:      21,
			Consolidator: consolidation.Sum,
			Datapoints: []schema.Point{
				{Ts: 10, Val: 10},
				{Ts: 20, Val: 35},
			},
		},
		{
			Interval:     10,
			QueryTo:      21,
			Consolidator: consolidation.Sum,
			Datapoints: []schema.Point{
				{Ts: 10, Val: 10},
				{Ts: 20, Val: 20},
			},
		},
	}
	dataMap := NewDataMap()
	got := Normalize(dataMap, in)
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
			QueryTo:      136,
			Consolidator: consolidation.Sum,
			Datapoints: []schema.Point{
				{Ts: 15, Val: 15},
				{Ts: 30, Val: 30},
				{Ts: 45, Val: 45},
				{Ts: 60, Val: 60},
				{Ts: 75, Val: 75},
				{Ts: 90, Val: 90},
				{Ts: 105, Val: 105},
				{Ts: 120, Val: 120},
				{Ts: 135, Val: 135},
			},
		},
		{
			Interval:     20,
			QueryTo:      136,
			Consolidator: consolidation.Sum,
			Datapoints: []schema.Point{
				{Ts: 20, Val: 20},
				{Ts: 40, Val: 40},
				{Ts: 60, Val: 60},
				{Ts: 80, Val: 80},
				{Ts: 100, Val: 100},
				{Ts: 120, Val: 120},
			},
		},
		{
			Interval:     10,
			QueryTo:      136,
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
				{Ts: 90, Val: 90},
				{Ts: 100, Val: 100},
				{Ts: 110, Val: 110},
				{Ts: 120, Val: 120},
				{Ts: 130, Val: 130},
			},
		},
	}
	want := []models.Series{
		{
			Interval:     60,
			QueryTo:      136,
			Consolidator: consolidation.Sum,
			Datapoints: []schema.Point{
				{Ts: 60, Val: 150},
				{Ts: 120, Val: 75 + 90 + 105 + 120},
			},
		},
		{
			Interval:     60,
			QueryTo:      136,
			Consolidator: consolidation.Sum,
			Datapoints: []schema.Point{
				{Ts: 60, Val: 120},
				{Ts: 120, Val: 80 + 100 + 120},
			},
		},
		{
			Interval:     60,
			QueryTo:      136,
			Consolidator: consolidation.Sum,
			Datapoints: []schema.Point{
				{Ts: 60, Val: 200},
				{Ts: 120, Val: 70 + 80 + 90 + 100 + 110 + 120},
			},
		},
	}
	dataMap := NewDataMap()
	got := Normalize(dataMap, in)
	fmt.Println("got:")
	fmt.Println(got[0].Datapoints)
	fmt.Println(got[1].Datapoints)
	if diff := cmp.Diff(want, got); diff != "" {
		t.Errorf("TestNormalize() mismatch (-want +got):\n%s", diff)
	}
}
