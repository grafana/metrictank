package expr

import (
	"testing"

	"github.com/grafana/metrictank/api/models"
)

func TestFallbackSeriesNo(t *testing.T) {
	testFallbackSeries(
		"NoFallback",
		[]models.Series{getQuerySeries("a", a)},
		[]models.Series{getQuerySeries("b", b)},
		[]models.Series{getQuerySeries("a", a)},
		t,
	)
}

func TestFallbackSeriesYes(t *testing.T) {
	testFallbackSeries(
		"zeroIn",
		[]models.Series{},
		[]models.Series{getQuerySeries("b", b)},
		[]models.Series{getQuerySeries("b", b)},
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

	// Copy input to check that it is unchanged later
	inputCopy := make([]models.Series, len(in))
	copy(inputCopy, in)
	fallbackCopy := make([]models.Series, len(fallback))
	copy(fallbackCopy, fallback)

	dataMap := DataMap(make(map[Req][]models.Series))

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
