package expr

import (
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
)

func TestAliasByNodeZero(t *testing.T) {
	testAliasByNode("zero", []models.Series{}, []models.Series{}, t)
}
func TestAliasByNodeSingle(t *testing.T) {
	testAliasByNode(
		"single",
		[]models.Series{
			getSeriesNamed("foo.bar.baz", a),
		},
		[]models.Series{
			getSeriesNamed("foo", a),
		},
		t,
	)
}

func TestAliasByNodeMultiple(t *testing.T) {
	testAliasByNode(
		"multiple",
		[]models.Series{
			getSeriesNamed("1.foo", a),
			getSeriesNamed("2.foo", b),
		},
		[]models.Series{
			getSeriesNamed("1", a),
			getSeriesNamed("2", b),
		},
		t,
	)
}

func makeAliasByNode(in []models.Series, nodes []expr) GraphiteFunc {
	f := NewAliasByNode()
	abn := f.(*FuncAliasByNode)
	abn.nodes = nodes
	abn.in = NewMock(in)
	return f
}

func testAliasByNode(name string, in []models.Series, out []models.Series, t *testing.T) {
	f := makeAliasByNode(in, []expr{{etype: etInt, int: 0}})

	// Copy input to check that it is unchanged later
	inputCopy := make([]models.Series, len(in))
	copy(inputCopy, in)

	dataMap := DataMap(make(map[Req][]models.Series))

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
}

func BenchmarkAliasByNode_1(b *testing.B) {
	benchmarkAliasByNode(b, 1)
}
func BenchmarkAliasByNode_10(b *testing.B) {
	benchmarkAliasByNode(b, 10)
}
func BenchmarkAliasByNode_100(b *testing.B) {
	benchmarkAliasByNode(b, 100)
}
func BenchmarkAliasByNode_1000(b *testing.B) {
	benchmarkAliasByNode(b, 1000)
}

func benchmarkAliasByNode(b *testing.B, numSeries int) {
	var input []models.Series
	for i := 0; i < numSeries; i++ {
		series := models.Series{
			Target: strconv.Itoa(i),
		}
		input = append(input, series)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := NewAliasByNode()
		aliasbynode := f.(*FuncAliasByNode)
		aliasbynode.nodes = []expr{{etype: etInt, int: 0}}
		aliasbynode.in = NewMock(input)
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
