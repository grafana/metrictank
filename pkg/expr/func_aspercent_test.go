package expr

import (
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
)

type errAsPercentNumSeriesMismatch struct {
	numIn    int
	numTotal int
}

func (e errAsPercentNumSeriesMismatch) Error() string {
	return fmt.Sprintf("asPercent got %d input series but %d total series (should  be same amount or 1)", e.numIn, e.numTotal)
}

func (e errAsPercentNumSeriesMismatch) HTTPStatusCode() int {
	return http.StatusBadRequest
}

var a1 = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: 50.6, Ts: 20},
	{Val: 1234567890, Ts: 30},
	{Val: math.NaN(), Ts: 40},
}

var b1 = []schema.Point{
	{Val: 10, Ts: 10},
	{Val: -25.8, Ts: 20},
	{Val: float64(math.MaxFloat64 / 1000), Ts: 30},
	{Val: math.NaN(), Ts: 40},
}
var a2 = []schema.Point{
	{Val: 10, Ts: 10},
	{Val: 25.3, Ts: 20},
	{Val: 12345678900, Ts: 30},
	{Val: -100, Ts: 40},
}

var b2 = []schema.Point{
	{Val: -1000, Ts: 10},
	{Val: 258, Ts: 20},
	{Val: math.MaxFloat64, Ts: 30},
	{Val: math.NaN(), Ts: 40},
}

var a1AsPercentOfa1 = []schema.Point{
	{Val: math.NaN(), Ts: 10},
	{Val: 100, Ts: 20},
	{Val: 100, Ts: 30},
	{Val: math.NaN(), Ts: 40},
}
var a1AsPercentOfa1b1 = []schema.Point{
	{Val: 0, Ts: 10}, // 100 * 0 / (0+10)
	{Val: float64(50.6) / float64(50.6-25.8) * 100, Ts: 20},
	{Val: float64(1234567890) / (float64(1234567890) + math.MaxFloat64/1000) * 100, Ts: 30},
	{Val: math.NaN(), Ts: 40},
}
var b1AsPercentOfa1b1 = []schema.Point{
	{Val: 100, Ts: 10},
	{Val: -100 * float64(25.8) / float64(50.6-25.8), Ts: 20},
	{Val: 100 * (math.MaxFloat64 / 1000) / (1234567890 + math.MaxFloat64/1000), Ts: 30},
	{Val: math.NaN(), Ts: 40},
}
var b1AsPercentOfa1 = []schema.Point{
	{Val: math.NaN(), Ts: 10},
	{Val: float64(-25.8) / 50.6 * 100, Ts: 20},
	{Val: float64(math.MaxFloat64/1000.0) / 1234567890 * 100, Ts: 30},
	{Val: math.NaN(), Ts: 40},
}

var a1AsPercentOfa2 = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: 200, Ts: 20},
	{Val: 10, Ts: 30},
	{Val: math.NaN(), Ts: 40},
}
var b1AsPercentOfb2 = []schema.Point{
	{Val: -1, Ts: 10},
	{Val: -10, Ts: 20},
	{Val: 0.1, Ts: 30},
	{Val: math.NaN(), Ts: 40},
}

var a1AsPercentOfFloat10 = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: float64(50.6) / 10.0 * 100, Ts: 20},
	{Val: 12345678900, Ts: 30},
	{Val: math.NaN(), Ts: 40},
}

// first, the case of no value for "total" specified, which should use the sum of all input series as total series
// let's try 3 different ways of inputting series

func TestAsPercentSingleInputUsingSelf(t *testing.T) {
	f, in := getNewAsPercent(
		[]models.Series{
			getSeries("a", "a", a1),
		})
	out := []models.Series{
		getSeries("asPercent(a,sumSeries(a))", "asPercent(a,sumSeries(a))", a1AsPercentOfa1),
	}

	execAndCheck(in, out, f, t)
}

func TestAsPercentDoubleInputUsingSelf(t *testing.T) {
	f, in := getNewAsPercent(
		[]models.Series{
			getSeries("a.a", "a.*", a1),
			getSeries("a.b", "a.*", b1),
		})
	out := []models.Series{
		getSeries("asPercent(a.a,sumSeries(a.*))", "asPercent(a.*,sumSeries(a.*))", a1AsPercentOfa1b1),
		getSeries("asPercent(a.b,sumSeries(a.*))", "asPercent(a.*,sumSeries(a.*))", b1AsPercentOfa1b1),
	}

	execAndCheck(in, out, f, t)
}

// now let's try the case of specifying another seriesList.
// specifically, 1 series, same amount of series as inputs (2), and different amount (3)
// for simplicity, each case uses 2 inputs.

func TestAsPercentDoubleInputUsingOne(t *testing.T) {
	f, in := getNewAsPercent(
		[]models.Series{
			getSeries("a.a", "a.*", a1),
			getSeries("a.b", "a.*", b1),
		})
	f.totalSeries = NewMock([]models.Series{getSeries("foo.a", "foo.*", a1)})
	out := []models.Series{
		getSeries("asPercent(a.a,foo.a)", "asPercent(a.*,foo.*)", a1AsPercentOfa1),
		getSeries("asPercent(a.b,foo.a)", "asPercent(a.*,foo.*)", b1AsPercentOfa1),
	}
	execAndCheck(in, out, f, t)
}
func TestAsPercentDoubleInputUsingTwo(t *testing.T) {
	f, in := getNewAsPercent(
		[]models.Series{
			getSeries("a.a", "a.*", a1),
			getSeries("a.b", "a.*", b1),
		})
	f.totalSeries = NewMock([]models.Series{
		getSeries("total.a", "total.*", a2),
		getSeries("total.b", "total.*", b2),
	})
	out := []models.Series{
		getSeries("asPercent(a.a,total.a)", "asPercent(a.*,total.*)", a1AsPercentOfa2),
		getSeries("asPercent(a.b,total.b)", "asPercent(a.*,total.*)", b1AsPercentOfb2),
	}
	execAndCheck(in, out, f, t)
}

// 2 input series, 3 total series -> not allowed!
func TestAsPercentDoubleInputUsingThree(t *testing.T) {
	f, _ := getNewAsPercent(
		[]models.Series{
			getSeries("a.a", "a.*", a1),
			getSeries("a.b", "a.*", b1),
		})
	f.totalSeries = NewMock([]models.Series{
		getSeries("total.a", "total.*", a2),
		getSeries("total.b", "total.*", b2),
		getSeries("total.c", "total.*", b2),
	})
	out := []models.Series{}
	got, err := f.Exec(make(map[Req][]models.Series))
	expErr := errAsPercentNumSeriesMismatch{2, 3}
	if err := equalOutput(out, got, expErr, err); err != nil {
		t.Fatal(err)
	}
}

// finally, test the case where an integer is specified as the total
// for simplicity, we'll just use 1 input.

func TestAsPercentSingleInputUsingFloat10(t *testing.T) {
	f, in := getNewAsPercent(
		[]models.Series{
			getSeries("a", "a", a1),
		})
	f.totalFloat = 10
	out := []models.Series{
		getSeries("asPercent(a,10)", "asPercent(a,10)", a1AsPercentOfFloat10),
	}
	execAndCheck(in, out, f, t)
}

func getNewAsPercent(in []models.Series) (*FuncAsPercent, []models.Series) {
	f := NewAsPercent()
	ps := f.(*FuncAsPercent)
	ps.in = NewMock(in)
	return ps, in
}

func execAndCheck(in, out []models.Series, f GraphiteFunc, t *testing.T) {
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
	t.Run("OutputIsCanonical", func(t *testing.T) {
		for i, s := range got {
			if !s.IsCanonical() {
				t.Fatalf("Case %s: output series %d is not canonical: %v", "main", i, s)
			}
		}
	})
}

func TestAsPercentSingleNoArg(t *testing.T) {

	f, in := getNewAsPercent(
		[]models.Series{
			getSeries("a;tag=something;tag2=anything", "func(tag=something;tag2=anything)", a),
		},
	)
	out := []models.Series{
		{
			Interval:  10,
			QueryFrom: 10,
			QueryTo:   61,
			QueryPatt: "asPercent(func(tag=something;tag2=anything),sumSeries(func(tag=something;tag2=anything)))",
			Target:    "asPercent(a;tag=something;tag2=anything,sumSeries(func(tag=something;tag2=anything)))",
			Datapoints: []schema.Point{
				{Val: math.NaN(), Ts: 10},
				{Val: math.NaN(), Ts: 20},
				{Val: 100, Ts: 30},
				{Val: math.NaN(), Ts: 40},
				{Val: math.NaN(), Ts: 50},
				{Val: 100, Ts: 60},
			},
		},
	}

	execAndCheck(in, out, f, t)

}

func TestAsPercentMultipleNoArg(t *testing.T) {
	out1 := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: 0, Ts: 20},
		{Val: 5.5 / (math.MaxFloat64 - 14.5) * 100, Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: math.NaN(), Ts: 50},
		{Val: 100, Ts: 60},
	}
	out2 := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: 100, Ts: 20},
		{Val: (math.MaxFloat64 - 20) / (math.MaxFloat64 - 14.5) * 100, Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: 100, Ts: 50},
		{Val: math.NaN(), Ts: 60},
	}

	f, in := getNewAsPercent(
		[]models.Series{
			getSeries("a;tag=something;tag2=anything", "func(tag=something;tag2=anything)", a),
			getSeries("b;tag=something;tag2=anything", "func(tag=something;tag2=anything)", b),
		},
	)
	out := []models.Series{
		{
			Interval:   10,
			QueryFrom:  10,
			QueryTo:    61,
			QueryPatt:  "asPercent(func(tag=something;tag2=anything),sumSeries(func(tag=something;tag2=anything)))",
			Target:     "asPercent(a;tag=something;tag2=anything,sumSeries(func(tag=something;tag2=anything)))",
			Datapoints: out1,
		},
		{
			Interval:   10,
			QueryFrom:  10,
			QueryTo:    61,
			QueryPatt:  "asPercent(func(tag=something;tag2=anything),sumSeries(func(tag=something;tag2=anything)))",
			Target:     "asPercent(b;tag=something;tag2=anything,sumSeries(func(tag=something;tag2=anything)))",
			Datapoints: out2,
		},
	}

	execAndCheck(in, out, f, t)
}

func TestAsPercentTotalFloat(t *testing.T) {
	f, in := getNewAsPercent(
		[]models.Series{
			getSeries("a;tag=something;tag2=anything", "func(a;tag=something;tag2=anything)", a),
		},
	)

	f.totalFloat = 123.456
	out := []models.Series{
		{
			Interval:  10,
			QueryFrom: 10,
			QueryTo:   61,
			QueryPatt: "asPercent(func(a;tag=something;tag2=anything),123.456)",
			Target:    "asPercent(a;tag=something;tag2=anything,123.456)",
			Datapoints: []schema.Point{
				{Val: 0, Ts: 10},
				{Val: 0, Ts: 20},
				{Val: 5.5 / f.totalFloat * 100, Ts: 30},
				{Val: math.NaN(), Ts: 40},
				{Val: math.NaN(), Ts: 50},
				{Val: 1234567890 / f.totalFloat * 100, Ts: 60},
			},
		},
	}
	execAndCheck(in, out, f, t)
}

func TestAsPercentTotalSerie(t *testing.T) {
	out1 := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: math.NaN(), Ts: 20},
		{Val: math.Inf(0), Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: math.NaN(), Ts: 50},
		{Val: math.NaN(), Ts: 60},
	}
	out2 := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: math.NaN(), Ts: 20},
		{Val: 199 / 5.5 * 100, Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: math.NaN(), Ts: 50},
		{Val: float64(250) / 1234567890 * 100, Ts: 60},
	}
	f, in := getNewAsPercent(
		[]models.Series{
			getSeries("b;tag=something;tag2=anything", "func(tag=something;tag2=anything)", b),
			getSeries("d;tag=something;tag2=anything", "func(tag=something;tag2=anything)", d),
		},
	)
	f.totalSeries = NewMock(
		[]models.Series{
			getSeries("a;tag=some;tag2=totalSerie", "func(tag=some;tag2=totalSerie)", a),
		},
	)
	out := []models.Series{
		{
			Interval:   10,
			QueryFrom:  10,
			QueryTo:    61,
			QueryPatt:  "asPercent(func(tag=something;tag2=anything),func(tag=some;tag2=totalSerie))",
			Target:     "asPercent(b;tag=something;tag2=anything,a;tag=some;tag2=totalSerie)",
			Datapoints: out1,
		},
		{
			Interval:   10,
			QueryFrom:  10,
			QueryTo:    61,
			QueryPatt:  "asPercent(func(tag=something;tag2=anything),func(tag=some;tag2=totalSerie))",
			Target:     "asPercent(d;tag=something;tag2=anything,a;tag=some;tag2=totalSerie)",
			Datapoints: out2,
		},
	}

	execAndCheck(in, out, f, t)
}

func TestAsPercentTotalSeries(t *testing.T) {
	out1 := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: math.NaN(), Ts: 20},
		{Val: math.Inf(0), Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: math.NaN(), Ts: 50},
		{Val: math.NaN(), Ts: 60},
	}
	out2 := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: math.NaN(), Ts: 20},
		{Val: float64(199) * 100, Ts: 30},
		{Val: float64(29) / 2 * 100, Ts: 40},
		{Val: float64(80) / 3.0 * 100, Ts: 50},
		{Val: float64(250) / 4 * 100, Ts: 60},
	}

	f, in := getNewAsPercent(
		[]models.Series{
			getSeries("b;tag=something;tag2=anything", "func(tag=something;tag2=anything)", b),
			getSeries("d;tag=something;tag2=anything", "func(tag=something;tag2=anything)", d),
		},
	)
	f.totalSeries = NewMock(
		[]models.Series{
			getSeries("c;tag=some;tag2=totalSerie", "func(tag=some;tag2=totalSerie)", c),
			getSeries("a;tag=some;tag2=totalSerie", "func(tag=some;tag2=totalSerie)", a),
		},
	)
	out := []models.Series{
		{
			Interval:   10,
			QueryFrom:  10,
			QueryTo:    61,
			QueryPatt:  "asPercent(func(tag=something;tag2=anything),func(tag=some;tag2=totalSerie))",
			Target:     "asPercent(b;tag=something;tag2=anything,a;tag=some;tag2=totalSerie)",
			Datapoints: out1,
		},
		{
			Interval:   10,
			QueryFrom:  10,
			QueryTo:    61,
			QueryPatt:  "asPercent(func(tag=something;tag2=anything),func(tag=some;tag2=totalSerie))",
			Target:     "asPercent(d;tag=something;tag2=anything,c;tag=some;tag2=totalSerie)",
			Datapoints: out2,
		},
	}

	execAndCheck(in, out, f, t)
}

func TestAsPercentNoArgNodes(t *testing.T) {
	out1 := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: 0, Ts: 20},
		{Val: 5.5 / (math.MaxFloat64 - 14.5) * 100, Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: math.NaN(), Ts: 50},
		{Val: 100, Ts: 60},
	}
	out2 := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: 100, Ts: 20},
		{Val: (math.MaxFloat64 - 20) / (math.MaxFloat64 - 14.5) * 100, Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: 100, Ts: 50},
		{Val: math.NaN(), Ts: 60},
	}
	out3 := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: math.NaN(), Ts: 20},
		{Val: 100, Ts: 30},
		{Val: 100, Ts: 40},
		{Val: 100, Ts: 50},
		{Val: 100, Ts: 60},
	}

	f, _ := getNewAsPercent(
		[]models.Series{
			getSeries("this.that.a;tag=something;tag2=anything", "func(tag=something;tag2=anything)", a),
			getSeries("this.that.b;tag=something;tag2=anything", "func(tag=something;tag2=anything)", b),
			getSeries("this.this.c;tag=something;tag2=anything", "func(tag=something;tag2=anything)", c),
		},
	)
	f.nodes = []expr{{etype: etFloat, float: 0}, {etype: etInt, int: 1}}
	out := []models.Series{
		{
			Interval:   10,
			QueryFrom:  10,
			QueryTo:    61,
			QueryPatt:  "asPercent(func(tag=something;tag2=anything),sumSeries(func(tag=something;tag2=anything)))",
			Target:     "asPercent(this.that.a;tag=something;tag2=anything,sumSeries(func(tag=something;tag2=anything)))",
			Datapoints: out1,
		},
		{
			Interval:   10,
			QueryFrom:  10,
			QueryTo:    61,
			QueryPatt:  "asPercent(func(tag=something;tag2=anything),sumSeries(func(tag=something;tag2=anything)))",
			Target:     "asPercent(this.that.b;tag=something;tag2=anything,sumSeries(func(tag=something;tag2=anything)))",
			Datapoints: out2,
		},
		{
			Interval:   10,
			QueryFrom:  10,
			QueryTo:    61,
			QueryPatt:  "asPercent(func(tag=something;tag2=anything),func(tag=something;tag2=anything))",
			Target:     "asPercent(this.this.c;tag=something;tag2=anything,this.this.c;tag=something;tag2=anything)",
			Datapoints: out3,
		},
	}

	got, err := f.Exec(make(map[Req][]models.Series))
	sort.Slice(got, func(i, j int) bool { return got[i].Target < got[j].Target })
	sort.Slice(out, func(i, j int) bool { return out[i].Target < out[j].Target })
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal(err)
	}

}

func TestAsPercentNoArgTagNodes(t *testing.T) {
	out1 := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: 0, Ts: 20},
		{Val: 5.5 / (math.MaxFloat64 - 14.5) * 100, Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: math.NaN(), Ts: 50},
		{Val: 100, Ts: 60},
	}
	out2 := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: 100, Ts: 20},
		{Val: (math.MaxFloat64 - 20) / (math.MaxFloat64 - 14.5) * 100, Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: 100, Ts: 50},
		{Val: math.NaN(), Ts: 60},
	}
	out3 := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: math.NaN(), Ts: 20},
		{Val: 100, Ts: 30},
		{Val: 100, Ts: 40},
		{Val: 100, Ts: 50},
		{Val: 100, Ts: 60},
	}

	f, _ := getNewAsPercent(
		[]models.Series{
			getSeries("this.that.a;tag=something1;tag2=anything", "func(tag=something1;tag2=anything)", a),
			getSeries("this.those.b;tag=something1;tag2=anything", "func(tag=something1;tag2=anything)", b),
			getSeries("this.this.c;tag=something2;tag2=anything", "func(tag=something2;tag2=anything)", c),
		},
	)
	f.nodes = []expr{{etype: etString, str: "tag"}}
	out := []models.Series{
		{
			Interval:   10,
			QueryFrom:  10,
			QueryTo:    61,
			QueryPatt:  "asPercent(func(tag=something1;tag2=anything),sumSeries(func(tag=something1;tag2=anything)))",
			Target:     "asPercent(this.that.a;tag=something1;tag2=anything,sumSeries(func(tag=something1;tag2=anything)))",
			Datapoints: out1,
		},
		{
			Interval:   10,
			QueryFrom:  10,
			QueryTo:    61,
			QueryPatt:  "asPercent(func(tag=something1;tag2=anything),sumSeries(func(tag=something1;tag2=anything)))",
			Target:     "asPercent(this.those.b;tag=something1;tag2=anything,sumSeries(func(tag=something1;tag2=anything)))",
			Datapoints: out2,
		},
		{
			Interval:   10,
			QueryFrom:  10,
			QueryTo:    61,
			QueryPatt:  "asPercent(func(tag=something2;tag2=anything),func(tag=something2;tag2=anything))",
			Target:     "asPercent(this.this.c;tag=something2;tag2=anything,this.this.c;tag=something2;tag2=anything)",
			Datapoints: out3,
		},
	}

	got, err := f.Exec(make(map[Req][]models.Series))
	sort.Slice(got, func(i, j int) bool { return got[i].Target < got[j].Target })
	sort.Slice(out, func(i, j int) bool { return out[i].Target < out[j].Target })
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal(err)
	}

}

func TestAsPercentSeriesByNodes(t *testing.T) {
	out1 := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: 0, Ts: 20},
		{Val: 5.5 / (math.MaxFloat64) * 100, Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: math.NaN(), Ts: 50},
		{Val: 1234567890.0 / 1234568148 * 100, Ts: 60},
	}
	out2 := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: 100, Ts: 20},
		{Val: 100, Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: 1234567890.0 / 1234567976 * 100, Ts: 50},
		{Val: math.NaN(), Ts: 60},
	}
	allNaN := []schema.Point{
		{Val: math.NaN(), Ts: 10},
		{Val: math.NaN(), Ts: 20},
		{Val: math.NaN(), Ts: 30},
		{Val: math.NaN(), Ts: 40},
		{Val: math.NaN(), Ts: 50},
		{Val: math.NaN(), Ts: 60},
	}
	f, _ := getNewAsPercent(
		[]models.Series{
			getSeries("this.that.a;tag=something;tag2=anything", "func(tag=something;tag2=anything)", a),
			getSeries("this.that.b;tag=something;tag2=anything", "func(tag=something;tag2=anything)", b),
			getSeries("this.this.c;tag=something;tag2=anything", "func(tag=something;tag2=anything)", c),
		},
	)
	f.totalSeries = NewMock(
		[]models.Series{
			getSeries("this.those.ab;tag=something;tag2=totalSerie", "func(tag=something;tag2=totalSerie)", sumab),
			getSeries("this.that.abc;tag=something;tag2=totalSerie", "func(tag=something;tag2=totalSerie)", sumabc),
			getSeries("this.that.cd;tag=something;tag2=totalSerie", "func(tag=something;tag2=totalSerie)", sumcd),
		},
	)
	f.nodes = []expr{{etype: etFloat, float: 0}, {etype: etInt, int: 1}}
	out := []models.Series{
		{
			Interval:   10,
			QueryFrom:  10,
			QueryTo:    61,
			QueryPatt:  "asPercent(func(tag=something;tag2=anything),sumSeries(func(tag=something;tag2=totalSerie)))",
			Target:     "asPercent(this.that.a;tag=something;tag2=anything,sumSeries(func(tag=something;tag2=totalSerie)))",
			Datapoints: out1,
		},
		{
			Interval:   10,
			QueryFrom:  10,
			QueryTo:    61,
			QueryPatt:  "asPercent(func(tag=something;tag2=anything),sumSeries(func(tag=something;tag2=totalSerie)))",
			Target:     "asPercent(this.that.b;tag=something;tag2=anything,sumSeries(func(tag=something;tag2=totalSerie)))",
			Datapoints: out2,
		},
		{
			Interval:   10,
			QueryFrom:  10,
			QueryTo:    61,
			QueryPatt:  "asPercent(func(tag=something;tag2=anything),MISSING)",
			Target:     "asPercent(this.this.c;tag=something;tag2=anything,MISSING)",
			Datapoints: allNaN,
		},
		{
			Interval:   10,
			QueryFrom:  10,
			QueryTo:    61,
			QueryPatt:  "asPercent(MISSING,func(tag=something;tag2=totalSerie))",
			Target:     "asPercent(MISSING,this.those.ab;tag=something;tag2=totalSerie)",
			Datapoints: allNaN,
		},
	}

	got, err := f.Exec(make(map[Req][]models.Series))
	sort.Slice(got, func(i, j int) bool { return got[i].Target < got[j].Target })
	sort.Slice(out, func(i, j int) bool { return out[i].Target < out[j].Target })
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal(err)
	}

}

func BenchmarkAsPercent10k_1NoNulls(b *testing.B) {
	benchmarkAsPercent(b, 1, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkAsPercent10k_10NoNulls(b *testing.B) {
	benchmarkAsPercent(b, 10, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkAsPercent10k_100NoNulls(b *testing.B) {
	benchmarkAsPercent(b, 100, test.RandFloats10k, test.RandFloats10k)
}
func BenchmarkAsPercent10k_1000NoNulls(b *testing.B) {
	benchmarkAsPercent(b, 1000, test.RandFloats10k, test.RandFloats10k)
}

func BenchmarkAsPercent10k_1SomeSeriesHalfNulls(b *testing.B) {
	benchmarkAsPercent(b, 1, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkAsPercent10k_10SomeSeriesHalfNulls(b *testing.B) {
	benchmarkAsPercent(b, 10, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkAsPercent10k_100SomeSeriesHalfNulls(b *testing.B) {
	benchmarkAsPercent(b, 100, test.RandFloats10k, test.RandFloatsWithNulls10k)
}
func BenchmarkAsPercent10k_1000SomeSeriesHalfNulls(b *testing.B) {
	benchmarkAsPercent(b, 1000, test.RandFloats10k, test.RandFloatsWithNulls10k)
}

func BenchmarkAsPercent10k_1AllSeriesHalfNulls(b *testing.B) {
	benchmarkAsPercent(b, 1, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkAsPercent10k_10AllSeriesHalfNulls(b *testing.B) {
	benchmarkAsPercent(b, 10, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkAsPercent10k_100AllSeriesHalfNulls(b *testing.B) {
	benchmarkAsPercent(b, 100, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}
func BenchmarkAsPercent10k_1000AllSeriesHalfNulls(b *testing.B) {
	benchmarkAsPercent(b, 1000, test.RandFloatsWithNulls10k, test.RandFloatsWithNulls10k)
}

func benchmarkAsPercent(b *testing.B, numSeries int, fn0, fn1 test.DataFunc) {
	var input []models.Series
	for i := 0; i < numSeries; i++ {
		series := models.Series{
			QueryPatt: strconv.Itoa(i),
		}
		if i%2 == 0 {
			series.Datapoints, series.Interval = fn0()
		} else {
			series.Datapoints, series.Interval = fn1()
		}
		input = append(input, series)
	}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f := NewAsPercent()
		f.(*FuncAsPercent).in = NewMock(input)
		got, err := f.Exec(make(map[Req][]models.Series))
		if err != nil {
			b.Fatalf("%s", err)
		}
		results = got
	}
}
