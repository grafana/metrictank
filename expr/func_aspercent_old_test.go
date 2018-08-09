package expr

import (
	"fmt"
	"math"
	"testing"

	"github.com/grafana/metrictank/api/models"
	"gopkg.in/raintank/schema.v1"
)

type errAsPercentNumSeriesMismatch struct {
	numIn    int
	numTotal int
}

func (e errAsPercentNumSeriesMismatch) Error() string {
	return fmt.Sprintf("asPercent got %d input series but %d total series (should  be same amount or 1)", e.numIn, e.numTotal)
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
	{Val: math.MaxFloat64 / 1000, Ts: 30},
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
	{Val: 100 * float64(50.6) / float64(50.6-25.8), Ts: 20},
	{Val: 100 * float64(1234567890) / (float64(1234567890) + math.MaxFloat64/1000), Ts: 30},
	{Val: math.NaN(), Ts: 40},
}
var b1AsPercentOfa1b1 = []schema.Point{
	{Val: 100, Ts: 10},
	{Val: -100 * float64(25.8) / float64(50.6-25.8), Ts: 20},
	{Val: 100 * (math.MaxFloat64 / 1000) / (1234567890 + math.MaxFloat64/1000), Ts: 30},
	{Val: math.NaN(), Ts: 40},
}
var a1AsPercentOfa1b1b1 = []schema.Point{
	{Val: 0, Ts: 10},
	{Val: 100 * 50.6 / (50.6 - 25.8 - 25.8), Ts: 20},
	{Val: 100 * 1234567890 / (float64(1234567890) + math.MaxFloat64/500), Ts: 30},
	{Val: math.NaN(), Ts: 40},
}
var b1AsPercentOfa1b1b1 = []schema.Point{
	{Val: 50, Ts: 10},
	{Val: -100 * 25.8 / (50.6 - 25.8 - 25.8), Ts: 20},
	{Val: (100 * math.MaxFloat64 / 1000) / (1234567890 + math.MaxFloat64/500), Ts: 30},
	{Val: math.NaN(), Ts: 40},
}

var b1AsPercentOfa1 = []schema.Point{
	{Val: math.MaxFloat64, Ts: 10},
	{Val: 100 * (-25.8) / 50.6, Ts: 20},
	{Val: 100 * (math.MaxFloat64 / float64(1000)) / float64(1234567890), Ts: 30},
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
	{Val: 506, Ts: 20},
	{Val: 12345678900, Ts: 30},
	{Val: math.NaN(), Ts: 40},
}

// first, the case of no value for "total" specified, which should use the sum of all input series as total series
// let's try 3 different ways of inputting series

func TestAsPercentSingleInputUsingSelf(t *testing.T) {
	f := getNewAsPercent(
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "a",
				Target:     "a",
				Datapoints: getCopy(a1),
			},
		})
	out := []models.Series{
		{
			Interval:   10,
			QueryPatt:  "asPercent(a)",
			Target:     "asPercent(a)",
			Datapoints: getCopy(a1AsPercentOfa1),
		},
	}
	got, err := f.Exec(make(map[Req][]models.Series))
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal(err)
	}
}

func TestAsPercentDoubleInputUsingSelf(t *testing.T) {
	f := getNewAsPercent(
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "a.*",
				Target:     "a.a",
				Datapoints: getCopy(a1),
			},
			{
				Interval:   10,
				QueryPatt:  "a.*",
				Target:     "a.b",
				Datapoints: getCopy(b1),
			},
		})
	out := []models.Series{
		{
			Interval:   10,
			QueryPatt:  "asPercent(a.*)",
			Target:     "asPercent(a.a)",
			Datapoints: getCopy(a1AsPercentOfa1b1),
		},
		{
			Interval:   10,
			QueryPatt:  "asPercent(a.*)",
			Target:     "asPercent(a.b)",
			Datapoints: getCopy(b1AsPercentOfa1b1),
		},
	}
	got, err := f.Exec(make(map[Req][]models.Series))
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal(err)
	}
}

// now let's try the case of specifying another seriesList.
// specifically, 1 series, same amount of series as inputs (2), and different amount (3)
// for simplicity, each case uses 2 inputs.

func TestAsPercentDoubleInputUsingOne(t *testing.T) {
	f := getNewAsPercent(
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "a.*",
				Target:     "a.a",
				Datapoints: getCopy(a1),
			},
			{
				Interval:   10,
				QueryPatt:  "a.*",
				Target:     "a.b",
				Datapoints: getCopy(b1),
			},
		})
	f.totalSeries = NewMock([]models.Series{{
		Interval:   10,
		QueryPatt:  "foo.*",
		Target:     "foo.a",
		Datapoints: getCopy(a1),
	}})
	out := []models.Series{
		{
			Interval:   10,
			QueryPatt:  "asPercent(a.*,foo.*)",
			Target:     "asPercent(a.a,foo.a)",
			Datapoints: getCopy(a1AsPercentOfa1),
		},
		{
			Interval:   10,
			QueryPatt:  "asPercent(a.*,foo.*)",
			Target:     "asPercent(a.b,foo.a)",
			Datapoints: getCopy(b1AsPercentOfa1),
		},
	}
	got, err := f.Exec(make(map[Req][]models.Series))
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal(err)
	}
}
func TestAsPercentDoubleInputUsingTwo(t *testing.T) {
	f := getNewAsPercent(
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "a.*",
				Target:     "a.a",
				Datapoints: getCopy(a1),
			},
			{
				Interval:   10,
				QueryPatt:  "a.*",
				Target:     "a.b",
				Datapoints: getCopy(b1),
			},
		})
	f.totalSeries = NewMock([]models.Series{
		{
			Interval:   10,
			QueryPatt:  "total.*",
			Target:     "total.a",
			Datapoints: getCopy(a2),
		},
		{
			Interval:   10,
			QueryPatt:  "total.*",
			Target:     "total.b",
			Datapoints: getCopy(b2),
		},
	})
	out := []models.Series{
		{
			Interval:   10,
			QueryPatt:  "asPercent(a.*,total.*)",
			Target:     "asPercent(a.a,total.a)",
			Datapoints: getCopy(a1AsPercentOfa2),
		},
		{
			Interval:   10,
			QueryPatt:  "asPercent(a.*,total.*)",
			Target:     "asPercent(a.b,total.b)",
			Datapoints: getCopy(b1AsPercentOfb2),
		},
	}
	got, err := f.Exec(make(map[Req][]models.Series))
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal(err)
	}
}

// 2 input series, 3 total series -> not allowed!
func TestAsPercentDoubleInputUsingThree(t *testing.T) {
	f := getNewAsPercent(
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "a.*",
				Target:     "a.a",
				Datapoints: getCopy(a1),
			},
			{
				Interval:   10,
				QueryPatt:  "a.*",
				Target:     "a.b",
				Datapoints: getCopy(b1),
			},
		})
	f.totalSeries = NewMock([]models.Series{
		{
			Interval:   10,
			QueryPatt:  "total.*",
			Target:     "total.a",
			Datapoints: getCopy(a2),
		},
		{
			Interval:   10,
			QueryPatt:  "total.*",
			Target:     "total.b",
			Datapoints: getCopy(b2),
		},
		{
			Interval:   10,
			QueryPatt:  "total.*",
			Target:     "total.c",
			Datapoints: getCopy(b2),
		},
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
	f := getNewAsPercent(
		[]models.Series{
			{
				Interval:   10,
				QueryPatt:  "a",
				Target:     "a",
				Datapoints: getCopy(a1),
			},
		})
	f.totalFloat = 10
	out := []models.Series{
		{
			Interval:   10,
			QueryPatt:  "asPercent(a,10)",
			Target:     "asPercent(a,10)",
			Datapoints: getCopy(a1AsPercentOfFloat10),
		},
	}
	got, err := f.Exec(make(map[Req][]models.Series))
	if err := equalOutput(out, got, nil, err); err != nil {
		t.Fatal(err)
	}
}

func getNewAsPercent(in []models.Series) *FuncAsPercent {
	f := NewAsPercent()
	ps := f.(*FuncAsPercent)
	ps.in = NewMock(in)
	return ps
}
