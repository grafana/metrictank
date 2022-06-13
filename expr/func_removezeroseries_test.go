package expr

import (
	"math"
	"testing"

	"github.com/grafana/metrictank/api/models"
)

func TestRemoveZeroSeriesIfAtLeastOneNonNull(t *testing.T) {
	testRemoveZeroSeries(
		0.0, // xFilesFactor
		[]models.Series{
			getSeries("a", "some zeros", a),
			getSeries("b", "half zeros", halfZeros),
			getSeries("d", "allZeros", allZeros),
		},
		[]models.Series{
			getSeries("a", "some zeros", a),
			getSeries("b", "half zeros", halfZeros),
		},
		t,
	)
}

func TestRemoveZeroSeriesAllowNoNulls(t *testing.T) {
	testRemoveZeroSeries(
		1.0, // xFilesFactor
		[]models.Series{
			getSeries("c", "no zeros", noZeros),
		},
		[]models.Series{
			getSeries("c", "no zeros", noZeros),
		},
		t,
	)
}

func TestRemoveZeroSeriesAllow30PercentNulls(t *testing.T) {
	testRemoveZeroSeries(
		0.3, // xFilesFactor
		[]models.Series{
			getSeries("a", "30% zeros", a),
			getSeries("b", "half zeros", halfZeros),
			getSeries("c", "no nulls", noZeros),
			getSeries("d", "all zeros", allZeros),
		},
		[]models.Series{
			getSeries("a", "30% zeros", a),
			getSeries("c", "no zeros", noZeros),
		},
		t,
	)
}

func TestRemoveZeroSeriesAllow70PercentNulls(t *testing.T) {
	testRemoveZeroSeries(
		0.7, // xFilesFactor
		[]models.Series{
			getSeries("a", "30% zeros", a),
			getSeries("b", "half zeros", halfZeros),
			getSeries("c", "no zeros", noZeros),
			getSeries("d", "all zeros", allZeros),
		},
		[]models.Series{
			getSeries("c", "no zeros", noZeros),
		},
		t,
	)
}

func TestRemoveZeroSeriesMissingInputXFilesFactor(t *testing.T) {
	testRemoveZeroSeries(
		math.NaN(), // xFilesFactor
		[]models.Series{
			getSeries("a", "some zeros", a),
			getSeries("b", "half zeros", halfZeros),
			getSeries("d", "allZeros", allZeros),
		},
		[]models.Series{
			getSeries("a", "some zeros", a),
			getSeries("b", "half zeros", halfZeros),
		},
		t,
	)
}

func testRemoveZeroSeries(xff float64, in []models.Series, out []models.Series, t *testing.T) {
	f := NewRemoveZeroSeries()
	f.(*FuncRemoveZeroSeries).in = NewMock(in)
	f.(*FuncRemoveZeroSeries).xFilesFactor = xff

	inputCopy := models.SeriesCopy(in) // to later verify that it is unchanged

	dataMap := initDataMap(in)

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
