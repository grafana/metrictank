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
			getSeries("a", "some nulls", a),
			getSeries("b", "all nulls", allNulls),
			getSeries("c", "no nulls", c),
			getSeries("d", "allZeros", allZeros),
		},
		[]models.Series{
			getSeries("a", "some nulls", a),
			getSeries("c", "no nulls", c),
			getSeries("d", "allZeros", allZeros),
		},
		t,
	)
}

func TestRemoveEmptySeriesAllowNoNulls(t *testing.T) {
	testRemoveEmptySeries(
		1.0, // xFilesFactor
		[]models.Series{
			getSeries("a", "some nulls", a),
			getSeries("b", "all nulls", allNulls),
			getSeries("c", "no nulls", c),
			getSeries("d", "allZeros", allZeros),
		},
		[]models.Series{
			getSeries("c", "no nulls", c),
			getSeries("d", "allZeros", allZeros),
		},
		t,
	)
}

func TestRemoveEmptySeriesAllow30PercentNulls(t *testing.T) {
	testRemoveEmptySeries(
		0.3, // xFilesFactor
		[]models.Series{
			getSeries("a", "30% nulls", a),
			getSeries("b", "all nulls", allNulls),
			getSeries("c", "no nulls", c),
			getSeries("d", "allZeros", allZeros),
		},
		[]models.Series{
			getSeries("a", "30% nulls", a),
			getSeries("c", "no nulls", c),
			getSeries("d", "allZeros", allZeros),
		},
		t,
	)
}

func TestRemoveEmptySeriesAllow70PercentNulls(t *testing.T) {
	testRemoveEmptySeries(
		0.7, // xFilesFactor
		[]models.Series{
			getSeries("a", "30% nulls", a),
			getSeries("b", "all nulls", allNulls),
			getSeries("c", "no nulls", c),
			getSeries("d", "allZeros", allZeros),
		},
		[]models.Series{
			getSeries("c", "no nulls", c),
			getSeries("d", "allZeros", allZeros),
		},
		t,
	)
}

func TestRemoveEmptySeriesMissingInputXFilesFactor(t *testing.T) {
	testRemoveEmptySeries(
		math.NaN(), // xFilesFactor
		[]models.Series{
			getSeries("a", "some nulls", a),
			getSeries("b", "all nulls", allNulls),
			getSeries("c", "no nulls", c),
			getSeries("d", "allZeros", allZeros),
		},
		[]models.Series{
			getSeries("a", "some nulls", a),
			getSeries("c", "no nulls", c),
			getSeries("d", "allZeros", allZeros),
		},
		t,
	)
}

func testRemoveEmptySeries(xff float64, in []models.Series, out []models.Series, t *testing.T) {
	f := NewRemoveEmptySeries()
	f.(*FuncRemoveEmptySeries).in = NewMock(in)
	f.(*FuncRemoveEmptySeries).xFilesFactor = xff

	// Copy input to check that it is unchanged later
	inputCopy := make([]models.Series, len(in))
	copy(inputCopy, in)

	dataMap := DataMap(make(map[Req][]models.Series))

	got, err := f.Exec(dataMap)
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal(err)
	}

	t.Run("DidNotModifyInput", func(t *testing.T) {
		if err := equalOutput(inputCopy, in, nil, nil); err != nil {
			t.Fatalf("Input was modified, err = %s", err)
		}
	})

	t.Run("DoesNotDoubleReturnPoints", func(t *testing.T) {
		if err := dataMap.CheckForOverlappingPoints(); err != nil {
			t.Fatalf("Point slices in datamap overlap, err = %s", err)
		}
	})
}
