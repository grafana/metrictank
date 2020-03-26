package expr

import (
	"math"
	"testing"

	"github.com/grafana/metrictank/api/models"
)

func TestRemoveEmptySeriesIfAtLeastOneNonNull(t *testing.T) {
	testRemoveEmptySeries(
		0.0, // xFilesFactor
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "some nulls",
				Target:     "a",
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				QueryPatt:  "all nulls",
				Target:     "b",
				Datapoints: getCopy(allNulls),
			},
			{
				Interval:   10,
				QueryPatt:  "no nulls",
				Target:     "c",
				Datapoints: getCopy(c),
			},
			{
				Interval:   10,
				QueryPatt:  "allZeros",
				Target:     "d",
				Datapoints: getCopy(allZeros),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				Target:     "removeEmptySeries(a, 0)",
				QueryPatt:  "removeEmptySeries(a, 0)",
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				Target:     "removeEmptySeries(c, 0)",
				QueryPatt:  "removeEmptySeries(c, 0)",
				Datapoints: getCopy(c),
			},
			{
				Interval:   10,
				Target:     "removeEmptySeries(d, 0)",
				QueryPatt:  "removeEmptySeries(d, 0)",
				Datapoints: getCopy(allZeros),
			},
		},
		t,
	)
}

func TestRemoveEmptySeriesAllowNoNulls(t *testing.T) {
	testRemoveEmptySeries(
		1.0, // xFilesFactor
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "some nulls",
				Target:     "a",
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				QueryPatt:  "all nulls",
				Target:     "b",
				Datapoints: getCopy(allNulls),
			},
			{
				Interval:   10,
				QueryPatt:  "no nulls",
				Target:     "c",
				Datapoints: getCopy(c),
			},
			{
				Interval:   10,
				QueryPatt:  "allZeros",
				Target:     "d",
				Datapoints: getCopy(allZeros),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				Target:     "removeEmptySeries(c, 1)",
				QueryPatt:  "removeEmptySeries(c, 1)",
				Datapoints: getCopy(c),
			},
			{
				Interval:   10,
				Target:     "removeEmptySeries(d, 1)",
				QueryPatt:  "removeEmptySeries(d, 1)",
				Datapoints: getCopy(allZeros),
			},
		},
		t,
	)
}

func TestRemoveEmptySeriesAllow30PercentNulls(t *testing.T) {
	testRemoveEmptySeries(
		0.3, // xFilesFactor
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "30 % nulls",
				Target:     "a",
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				QueryPatt:  "all nulls",
				Target:     "b",
				Datapoints: getCopy(allNulls),
			},
			{
				Interval:   10,
				QueryPatt:  "no nulls",
				Target:     "c",
				Datapoints: getCopy(c),
			},
			{
				Interval:   10,
				QueryPatt:  "allZeros",
				Target:     "d",
				Datapoints: getCopy(allZeros),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				Target:     "removeEmptySeries(a, 0.3)",
				QueryPatt:  "removeEmptySeries(a, 0.3)",
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				Target:     "removeEmptySeries(c, 0.3)",
				QueryPatt:  "removeEmptySeries(c, 0.3)",
				Datapoints: getCopy(c),
			},
			{
				Interval:   10,
				Target:     "removeEmptySeries(d, 0.3)",
				QueryPatt:  "removeEmptySeries(d, 0.3)",
				Datapoints: getCopy(allZeros),
			},
		},
		t,
	)
}

func TestRemoveEmptySeriesAllow70PercentNulls(t *testing.T) {
	testRemoveEmptySeries(
		0.7, // xFilesFactor
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "30 % nulls",
				Target:     "a",
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				QueryPatt:  "all nulls",
				Target:     "b",
				Datapoints: getCopy(allNulls),
			},
			{
				Interval:   10,
				QueryPatt:  "no nulls",
				Target:     "c",
				Datapoints: getCopy(c),
			},
			{
				Interval:   10,
				QueryPatt:  "allZeros",
				Target:     "d",
				Datapoints: getCopy(allZeros),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				Target:     "removeEmptySeries(c, 0.7)",
				QueryPatt:  "removeEmptySeries(c, 0.7)",
				Datapoints: getCopy(c),
			},
			{
				Interval:   10,
				Target:     "removeEmptySeries(d, 0.7)",
				QueryPatt:  "removeEmptySeries(d, 0.7)",
				Datapoints: getCopy(allZeros),
			},
		},
		t,
	)
}

func TestRemoveEmptySeriesInvalidInputXFilesFactor(t *testing.T) {

	xffs := []float64{-0.5, 50.0, math.MaxFloat64, math.Inf(1), math.Inf(-1)}
	for _, xff := range xffs {
		testRemoveEmptySeries(
			xff, // xFilesFactor
			[]models.Series{
				{
					Interval:   10,
					QueryPatt:  "30 % nulls",
					Target:     "a",
					Datapoints: getCopy(a),
				},
				{
					Interval:   10,
					QueryPatt:  "all nulls",
					Target:     "b",
					Datapoints: getCopy(allNulls),
				},
				{
					Interval:   10,
					QueryPatt:  "no nulls",
					Target:     "c",
					Datapoints: getCopy(c),
				},
				{
					Interval:   10,
					QueryPatt:  "allZeros",
					Target:     "d",
					Datapoints: getCopy(allZeros),
				},
			},
			[]models.Series{},
			t,
		)
	}
}

func TestRemoveEmptySeriesMissingInputXFilesFactor(t *testing.T) {
	testRemoveEmptySeries(
		math.NaN(), // xFilesFactor
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "some nulls",
				Target:     "a",
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				QueryPatt:  "all nulls",
				Target:     "b",
				Datapoints: getCopy(allNulls),
			},
			{
				Interval:   10,
				QueryPatt:  "no nulls",
				Target:     "c",
				Datapoints: getCopy(c),
			},
			{
				Interval:   10,
				QueryPatt:  "allZeros",
				Target:     "d",
				Datapoints: getCopy(allZeros),
			},
		},
		[]models.Series{
			{
				Interval:   10,
				Target:     "removeEmptySeries(a, 0)",
				QueryPatt:  "removeEmptySeries(a, 0)",
				Datapoints: getCopy(a),
			},
			{
				Interval:   10,
				Target:     "removeEmptySeries(c, 0)",
				QueryPatt:  "removeEmptySeries(c, 0)",
				Datapoints: getCopy(c),
			},
			{
				Interval:   10,
				Target:     "removeEmptySeries(d, 0)",
				QueryPatt:  "removeEmptySeries(d, 0)",
				Datapoints: getCopy(allZeros),
			},
		},
		t,
	)
}

func testRemoveEmptySeries(xff float64, in []models.Series, out []models.Series, t *testing.T) {
	f := NewRemoveEmptySeries()
	f.(*FuncRemoveEmptySeries).in = NewMock(in)
	f.(*FuncRemoveEmptySeries).xFilesFactor = xff

	got, err := f.Exec(make(map[Req][]models.Series))
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal(err)
	}
}
