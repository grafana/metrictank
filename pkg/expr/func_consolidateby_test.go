package expr

import (
	"testing"

	"github.com/grafana/metrictank/pkg/consolidation"

	"github.com/grafana/metrictank/pkg/api/models"
)

func TestConsolidateByZero(t *testing.T) {
	testConsolidateBy("zero", []models.Series{}, []models.Series{}, t)
}
func TestConsolidateBySingle(t *testing.T) {
	testConsolidateBy(
		"single",
		[]models.Series{
			getSeriesNamed("foo", a),
		},
		[]models.Series{
			getSeriesNamed("consolidateBy(foo,\"sum\")", a),
		},
		t,
	)
}
func TestConsolidateByMultiple(t *testing.T) {
	testConsolidateBy(
		"multiple",
		[]models.Series{
			getSeriesNamed("foo-1", a),
			getSeriesNamed("foo-2", b),
		},
		[]models.Series{
			getSeriesNamed("consolidateBy(foo-1,\"sum\")", a),
			getSeriesNamed("consolidateBy(foo-2,\"sum\")", b),
		},
		t,
	)
}

func makeConsolidateBy(in []models.Series) GraphiteFunc {
	f := NewConsolidateBy()
	consolidateby := f.(*FuncConsolidateBy)
	consolidateby.by = "sum"
	consolidateby.in = NewMock(in)
	return f
}

func testConsolidateBy(name string, in []models.Series, out []models.Series, t *testing.T) {
	f := makeConsolidateBy(in)

	// set sum consolidator for output
	for i := range out {
		out[i].QueryCons = consolidation.Sum
		out[i].Consolidator = consolidation.Sum
	}

	inputCopy := models.SeriesCopy(in) // to later verify that it is unchanged

	dataMap := initDataMap(in)

	got, err := f.Exec(dataMap)
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatalf("Case %s: %s", name, err)
	}

	t.Run("DidNotModifyInput", func(t *testing.T) {
		if err := equalOutput(inputCopy, in, nil, nil); err != nil {
			t.Fatalf("Case %s: Input was modified, err = %s", name, err)
		}

	})

	t.Run("DoesNotDoubleReturnPoints", func(t *testing.T) {
		if err := dataMap.CheckForOverlappingPoints(); err != nil {
			t.Fatalf("Case %s: Point slices in datamap overlap, err = %s", name, err)
		}
	})
	t.Run("OutputIsCanonical", func(t *testing.T) {
		for i, s := range got {
			if !s.IsCanonical() {
				t.Fatalf("Case %s: output series %d is not canonical: %v", name, i, s)
			}
		}
	})
}
