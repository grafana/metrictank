package expr

import (
	"testing"

	"github.com/grafana/metrictank/pkg/api/models"
)

func TestFallbackSeriesNo(t *testing.T) {
	testFallbackSeries(
		"NoFallback",
		[]models.Series{getSeriesNamed("a", a)},
		[]models.Series{getSeriesNamed("b", b)},
		[]models.Series{getSeriesNamed("a", a)},
		t,
	)
}

func TestFallbackSeriesYes(t *testing.T) {
	testFallbackSeries(
		"zeroIn",
		[]models.Series{},
		[]models.Series{getSeriesNamed("b", b)},
		[]models.Series{getSeriesNamed("b", b)},
		t,
	)
}

func makeFallbackSeries(in []models.Series, fallback []models.Series) *FuncFallbackSeries {
	f := NewFallbackSeries()
	s := f.(*FuncFallbackSeries)
	s.in = NewMock(in)
	s.fallback = NewMock(fallback)
	return s
}

func testFallbackSeries(name string, in, fallback, out []models.Series, t *testing.T) {
	f := makeFallbackSeries(in, fallback)

	inputCopy := models.SeriesCopy(in)          // to later verify that it is unchanged
	fallbackCopy := models.SeriesCopy(fallback) // to later verify that it is unchanged

	dataMap := initDataMap(in)

	got, err := f.Exec(dataMap)
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatalf("Case %s: %s", name, err)
	}

	t.Run("DidNotModifyInput", func(t *testing.T) {
		if err := equalOutput(inputCopy, in, nil, nil); err != nil {
			t.Fatalf("Case %s: Input was modified, err = %s", name, err)
		}
		if err := equalOutput(fallbackCopy, fallback, nil, nil); err != nil {
			t.Fatalf("Case %s: Input was modified, err = %s", name, err)
		}
	})

	t.Run("DoesNotDoubleReturnPoints", func(t *testing.T) {
		if err := dataMap.CheckForOverlappingPoints(); err != nil {
			t.Fatalf("Case %s: Point slices in datamap overlap, err = %s", name, err)
		}
	})
}
