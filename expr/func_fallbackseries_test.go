package expr

import (
	"testing"

	"github.com/grafana/metrictank/api/models"
)

func getNewFallbackSeries(in []models.Series, fallback []models.Series) *FuncFallbackSeries {
	f := NewFallbackSeries()
	s := f.(*FuncFallbackSeries)
	s.in = NewMock(in)
	s.fallback = NewMock(fallback)
	return s
}

func TestFallbackSeriesNo(t *testing.T) {
	f := getNewFallbackSeries(
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "a",
				Target:     "a",
				Datapoints: getCopy(a),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "b",
				Target:     "b",
				Datapoints: getCopy(b),
			},
		},
	)
	out := []models.Series{
		{
			Interval:   10,
			QueryPatt:  "a",
			Target:     "a",
			Datapoints: getCopy(a),
		},
	}

	got, err := f.Exec(make(map[Req][]models.Series))
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal(err)
	}
}

func TestFallbackSeriesYes(t *testing.T) {
	f := getNewFallbackSeries(
		[]models.Series{},
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "b",
				Target:     "b",
				Datapoints: getCopy(b),
			},
		},
	)
	out := []models.Series{
		{
			Interval:   10,
			QueryPatt:  "b",
			Target:     "b",
			Datapoints: getCopy(b),
		},
	}

	got, err := f.Exec(make(map[Req][]models.Series))
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal(err)
	}
}
