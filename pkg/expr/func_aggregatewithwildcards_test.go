package expr

import (
	"testing"

	"github.com/grafana/metrictank/pkg/api/models"
)

func TestAggregateWithWildcardsZero(t *testing.T) {
	f := makeAggregateWithWildcards("sum", []models.Series{}, 0, []expr{{etype: etInt, int: 1}})
	got, err := f.Exec(make(map[Req][]models.Series))
	if err := equalOutput([]models.Series{}, got, nil, err); err != nil {
		t.Fatal(err)
	}
}

func TestAggregateWithWildcardsIdentity(t *testing.T) {
	testAggregateWithWildcards(
		"identity",
		"average",
		[]models.Series{
			getSeriesNamed("single", a),
		},
		getSeriesNamed("single", a),
		t,
		0,
		[]expr{},
	)
	testAggregateWithWildcards(
		"identity",
		"sum",
		[]models.Series{
			getSeriesNamed("single", a),
		},
		getSeriesNamed("single", a),
		t,
		0,
		[]expr{},
	)
}
func TestAggregateWithWildcardsAvg(t *testing.T) {
	testAggregateWithWildcards(
		"avgSeries",
		"average",
		[]models.Series{
			getSeriesNamed("foo.fighters.bar", a),
			getSeriesNamed("foo.rock.bar", b),
		},
		getSeriesNamed("foo.bar", avgab),
		t,
		0,
		[]expr{{etype: etInt, int: 1}},
	)
}

func TestAggregateWithWildcardsSum(t *testing.T) {
	testAggregateWithWildcards(
		"sumSeries",
		"sum",
		[]models.Series{
			getSeriesNamed("foo.hello.bar", a),
			getSeriesNamed("foo.world.bar", b),
		},
		getSeriesNamed("foo.bar", sumab),
		t,
		0,
		[]expr{{etype: etInt, int: 1}},
	)
}

func TestAggregateWithWildcardsMultiply(t *testing.T) {
	testAggregateWithWildcards(
		"multiplySeries",
		"multiply",
		[]models.Series{
			getSeriesNamed("foo.hello.world.bar", a),
			getSeriesNamed("foo.something.else.bar", b),
		},
		getSeriesNamed("foo.bar", multab),
		t,
		0,
		[]expr{
			{etype: etInt, int: 1},
			{etype: etInt, int: 2},
		},
	)
}

func makeAggregateWithWildcards(agg string, in []models.Series, xFilesFactor float64, nodes []expr) GraphiteFunc {
	f := NewAggregateWithWildcardsConstructor(agg)()
	aggww := f.(*FuncAggregateWithWildcards)
	aggww.in = NewMock(in)
	aggww.nodes = nodes
	return f
}

func testAggregateWithWildcards(name, agg string, in []models.Series, out models.Series, t *testing.T, xFilesFactor float64, nodes []expr) {
	inputCopy := models.SeriesCopy(in) // to later verify that it is unchanged

	f := makeAggregateWithWildcards(agg, in, xFilesFactor, nodes)

	dataMap := initDataMap(in)

	got, err := f.Exec(dataMap)
	if err := equalOutput([]models.Series{out}, got, nil, err); err != nil {
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
