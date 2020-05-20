package expr

import (
	"math"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
)

func getNamedSeries(target, patt string, data []schema.Point) models.Series {
	return models.Series{
		Target:     target,
		QueryPatt:  patt,
		Datapoints: getCopy(data),
		Interval:   10,
	}
}

func TestRemoveEmptySeriesIfAtLeastOneNonNull(t *testing.T) {
	testRemoveEmptySeries(
		0.0, // xFilesFactor
		[]models.Series{
			getNamedSeries("a", "some nulls", a),
			getNamedSeries("b", "all nulls", allNulls),
			getNamedSeries("c", "no nulls", c),
			getNamedSeries("d", "allZeros", allZeros),
		},
		[]models.Series{
			getNamedSeries("a", "some nulls", a),
			getNamedSeries("c", "no nulls", c),
			getNamedSeries("d", "allZeros", allZeros),
		},
		t,
	)
}

func TestRemoveEmptySeriesAllowNoNulls(t *testing.T) {
	testRemoveEmptySeries(
		1.0, // xFilesFactor
		[]models.Series{
			getNamedSeries("a", "some nulls", a),
			getNamedSeries("b", "all nulls", allNulls),
			getNamedSeries("c", "no nulls", c),
			getNamedSeries("d", "allZeros", allZeros),
		},
		[]models.Series{
			getNamedSeries("c", "no nulls", c),
			getNamedSeries("d", "allZeros", allZeros),
		},
		t,
	)
}

func TestRemoveEmptySeriesAllow30PercentNulls(t *testing.T) {
	testRemoveEmptySeries(
		0.3, // xFilesFactor
		[]models.Series{
			getNamedSeries("a", "30% nulls", a),
			getNamedSeries("b", "all nulls", allNulls),
			getNamedSeries("c", "no nulls", c),
			getNamedSeries("d", "allZeros", allZeros),
		},
		[]models.Series{
			getNamedSeries("a", "30% nulls", a),
			getNamedSeries("c", "no nulls", c),
			getNamedSeries("d", "allZeros", allZeros),
		},
		t,
	)
}

func TestRemoveEmptySeriesAllow70PercentNulls(t *testing.T) {
	testRemoveEmptySeries(
		0.7, // xFilesFactor
		[]models.Series{
			getNamedSeries("a", "30% nulls", a),
			getNamedSeries("b", "all nulls", allNulls),
			getNamedSeries("c", "no nulls", c),
			getNamedSeries("d", "allZeros", allZeros),
		},
		[]models.Series{
			getNamedSeries("c", "no nulls", c),
			getNamedSeries("d", "allZeros", allZeros),
		},
		t,
	)
}

func TestRemoveEmptySeriesMissingInputXFilesFactor(t *testing.T) {
	testRemoveEmptySeries(
		math.NaN(), // xFilesFactor
		[]models.Series{
			getNamedSeries("a", "some nulls", a),
			getNamedSeries("b", "all nulls", allNulls),
			getNamedSeries("c", "no nulls", c),
			getNamedSeries("d", "allZeros", allZeros),
		},
		[]models.Series{
			getNamedSeries("a", "some nulls", a),
			getNamedSeries("c", "no nulls", c),
			getNamedSeries("d", "allZeros", allZeros),
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
