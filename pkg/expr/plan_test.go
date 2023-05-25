package expr

import (
	"errors"
	"math"
	"reflect"
	"testing"

	"github.com/google/go-cmp/cmp"
	"github.com/grafana/metrictank/internal/consolidation"
	"github.com/grafana/metrictank/pkg/api/models"
)

// TestArgs tests that after planning the given args against smartSummarize, the right error or requests come out
// here we use smartSummarize because it has multiple optional arguments which allows us to test some interesting things
func TestArgs(t *testing.T) {

	from := uint32(1000)
	to := uint32(2000)
	stable := true

	cases := []struct {
		name      string
		args      []*expr
		namedArgs map[string]*expr
		expReq    []Req
		expErrMsg string
		expErrIs  error
	}{
		{
			"2 args normal, 0 optional",
			[]*expr{
				{etype: etName, str: "foo.bar.*"},
				{etype: etString, str: "1hour"},
			},
			nil,
			[]Req{
				NewReq("foo.bar.*", from, to, 0, 0, 0),
			},
			"",
			nil,
		},
		{
			"2 args normal, 2 optional by position",
			[]*expr{
				{etype: etName, str: "foo.bar.*"},
				{etype: etString, str: "1hour"},
				{etype: etString, str: "sum"},
				{etype: etBool, bool: true},
			},
			nil,
			[]Req{
				NewReq("foo.bar.*", from, to, 0, 0, 0),
			},
			"",
			nil,
		},
		{
			"2 args normal, 2 optional by position (bools as strings)",
			[]*expr{
				{etype: etName, str: "foo.bar.*"},
				{etype: etString, str: "1hour"},
				{etype: etString, str: "sum"},
				{etype: etString, str: "false"},
			},
			nil,
			[]Req{
				NewReq("foo.bar.*", from, to, 0, 0, 0),
			},
			"",
			nil,
		},
		{
			"2 args normal, 2 optional by key",
			[]*expr{
				{etype: etName, str: "foo.bar.*"},
				{etype: etString, str: "1hour"},
			},
			map[string]*expr{
				"func":        {etype: etString, str: "sum"},
				"alignToFrom": {etype: etBool, bool: true},
			},
			[]Req{
				NewReq("foo.bar.*", from, to, 0, 0, 0),
			},
			"",
			nil,
		},
		{
			"2 args normal, 2 optional by key (bools as strings)",
			[]*expr{
				{etype: etName, str: "foo.bar.*"},
				{etype: etString, str: "1hour"},
			},
			map[string]*expr{
				"func":        {etype: etString, str: "sum"},
				"alignToFrom": {etype: etString, str: "true"},
			},
			[]Req{
				NewReq("foo.bar.*", from, to, 0, 0, 0),
			},
			"",
			nil,
		},
		{
			"2 args normal, 1 by position, 1 by keyword",
			[]*expr{
				{etype: etName, str: "foo.bar.*"},
				{etype: etString, str: "1hour"},
				{etype: etString, str: "sum"},
			},
			map[string]*expr{
				"alignToFrom": {etype: etBool, bool: true},
			},
			[]Req{
				NewReq("foo.bar.*", from, to, 0, 0, 0),
			},
			"",
			nil,
		},
		{
			"2 args normal, 2 by position, 1 by keyword (duplicate!)",
			[]*expr{
				{etype: etName, str: "foo.bar.*"},
				{etype: etString, str: "1hour"},
				{etype: etString, str: "sum"},
				{etype: etBool, bool: true},
			},
			map[string]*expr{
				"alignToFrom": {etype: etBool, bool: true},
			},
			nil,
			`can't plan function "smartSummarize": keyword argument "alignToFrom" specified twice`,
			ErrKwargSpecifiedTwice{"alignToFrom"},
		},
		{
			"2 args normal, 1 by position, 2 by keyword (duplicate!)",
			[]*expr{
				{etype: etName, str: "foo.bar.*"},
				{etype: etString, str: "1hour"},
				{etype: etString, str: "sum"},
			},
			map[string]*expr{
				"func":        {etype: etString, str: "sum"},
				"alignToFrom": {etype: etBool, bool: true},
			},
			nil,
			`can't plan function "smartSummarize": keyword argument "func" specified twice`,
			ErrKwargSpecifiedTwice{"func"},
		},
		{
			"2 args normal, 0 by position, the first by keyword",
			[]*expr{
				{etype: etName, str: "foo.bar.*"},
				{etype: etString, str: "1hour"},
			},
			map[string]*expr{
				"func": {etype: etString, str: "sum"},
			},
			[]Req{
				NewReq("foo.bar.*", from, to, 0, 0, 0),
			},
			"",
			nil,
		},
		{
			"2 args normal, 0 by position, the second by keyword",
			[]*expr{
				{etype: etName, str: "foo.bar.*"},
				{etype: etString, str: "1hour"},
			},
			map[string]*expr{
				"alignToFrom": {etype: etBool, bool: true},
			},
			[]Req{
				NewReq("foo.bar.*", from, to, 0, 0, 0),
			},
			"",
			nil,
		},
		{
			"missing required argument",
			[]*expr{
				{etype: etName, str: "foo.bar.*"},
			},
			nil,
			nil,
			`can't plan function "smartSummarize": argument missing`,
			ErrMissingArg,
		},
		{
			"2 args normal, 1 by position, 1 by keyword (unknown!)",
			[]*expr{
				{etype: etName, str: "foo.bar.*"},
				{etype: etString, str: "1hour"},
				{etype: etString, str: "sum"},
			},
			map[string]*expr{
				"unknownArg": {etype: etBool, bool: true},
			},
			nil,
			`can't plan function "smartSummarize", kwarg "unknownArg": unknown keyword argument "unknownArg"`,
			ErrUnknownKwarg{"unknownArg"},
		},
	}

	fn := NewSmartSummarize()
	for i, c := range cases {
		e := &expr{
			etype:     etFunc,
			str:       "smartSummarize",
			args:      c.args,
			namedArgs: c.namedArgs,
		}
		req, err := newplanFunc(e, fn, Context{from: from, to: to}, stable, nil)
		if c.expErrMsg == "" {
			if err != nil {
				t.Errorf("case %d: %q, expected no error, but got '%s'", i, c.name, err.Error())
			}
		} else {
			if !errors.Is(err, c.expErrIs) {
				t.Errorf("case %d: %q, expected error %v in wrapped error chain", i, c.name, c.expErrIs)
			}
			if c.expErrMsg != err.Error() {
				t.Errorf("case %d: %q, expected error message '%v' - got '%v'", i, c.name, c.expErrMsg, err.Error())
			}
		}
		if !reflect.DeepEqual(req, c.expReq) {
			t.Errorf("case %d: %q, expected req %v - got %v", i, c.name, c.expReq, req)
		}
	}
}

// TestArgQuotedInt tests that a function (perSecond in this case) can be given a quoted integer argument
func TestArgQuotedInt(t *testing.T) {
	cases := []expr{
		{etype: etInt, int: 5},
		{etype: etString, str: "5"},
	}
	for _, ourArg := range cases {
		fn := NewPerSecond()
		e := &expr{
			etype: etFunc,
			str:   "perSecond",
			args: []*expr{
				{etype: etName, str: "in.*"},
				&ourArg,
			},
			namedArgs: nil,
		}
		_, err := newplanFunc(e, fn, Context{from: 0, to: 1000}, true, nil)
		if err != nil {
			t.Fatal(err)
		}
		ap := fn.(*FuncPerSecond)
		if ap.maxValue != 5 {
			t.Fatalf("maxValue should be 5. got %d", ap.maxValue)
		}
	}
}

// TestArgQuotedFloat tests that a function (asPercent in this case) can be given a quoted floating point argument
func TestArgQuotedFloat(t *testing.T) {
	cases := []expr{
		{etype: etFloat, float: 5.0},
		{etype: etString, str: "5.0"},
		{etype: etFloat, float: -5.0},
		{etype: etString, str: "-5.0"},
	}
	results := []float64{
		5,
		5,
		-5,
		-5,
	}
	for i, ourArg := range cases {
		fn := NewAsPercent()
		e := &expr{
			etype: etFunc,
			str:   "asPercent",
			args: []*expr{
				{etype: etName, str: "in.*"},
				&ourArg,
			},
			namedArgs: nil,
		}
		_, err := newplanFunc(e, fn, Context{from: 0, to: 1000}, true, nil)
		if err != nil {
			t.Fatal(err)
		}
		ap := fn.(*FuncAsPercent)
		if ap.totalFloat != results[i] {
			t.Fatalf("totalFloat should be %f. got %f", results[i], ap.totalFloat)
		}
	}
}

// for the ArgIn tests, we use perSecond because it has a nice example of an ArgIn that can be specified via position or keyword.
func TestArgInMissing(t *testing.T) {
	fn := NewAsPercent()
	e := &expr{
		etype: etFunc,
		str:   "perSecond",
		args: []*expr{
			{etype: etName, str: "in.*"},
		},
		namedArgs: nil,
	}
	_, err := newplanFunc(e, fn, Context{from: 0, to: 1000}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	ap := fn.(*FuncAsPercent)
	if !math.IsNaN(ap.totalFloat) {
		t.Fatalf("totalFloat should be unset. got %f", ap.totalFloat)
	}
	if ap.totalSeries != nil {
		t.Fatalf("totalSeries should be nil. got %v", ap.totalSeries)
	}
}

func TestArgInSeriesPositional(t *testing.T) {
	fn := NewAsPercent()
	e := &expr{
		etype: etFunc,
		str:   "perSecond",
		args: []*expr{
			{etype: etName, str: "in.*"},
			{etype: etName, str: "total.*"},
		},
		namedArgs: nil,
	}
	_, err := newplanFunc(e, fn, Context{from: 0, to: 1000}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	ap := fn.(*FuncAsPercent)
	if !math.IsNaN(ap.totalFloat) {
		t.Fatalf("totalFloat should be unset. got %f", ap.totalFloat)
	}
	if ap.totalSeries == nil {
		t.Fatalf("totalSeries must not be nil. got nil")
	}
}

func TestArgInSeriesPositionalNone(t *testing.T) {
	fn := NewAsPercent()
	e := &expr{
		etype: etFunc,
		str:   "perSecond",
		args: []*expr{
			{etype: etName, str: "in.*"},
			{etype: etName, str: "None"},
		},
		namedArgs: nil,
	}
	_, err := newplanFunc(e, fn, Context{from: 0, to: 1000}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	ap := fn.(*FuncAsPercent)
	if !math.IsNaN(ap.totalFloat) {
		t.Fatalf("totalFloat should be unset. got %f", ap.totalFloat)
	}
	if ap.totalSeries != nil {
		t.Fatalf("totalSeries must be nil. got %v", ap.totalSeries)
	}
}

func TestArgInIntPositional(t *testing.T) {
	fn := NewAsPercent()
	e := &expr{
		etype: etFunc,
		str:   "perSecond",
		args: []*expr{
			{etype: etName, str: "in.*"},
			{etype: etInt, str: "10", int: 10},
		},
		namedArgs: nil,
	}
	_, err := newplanFunc(e, fn, Context{from: 0, to: 1000}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	ap := fn.(*FuncAsPercent)
	if ap.totalFloat != 10 {
		t.Fatalf("totalFloat should be 10. got %f", ap.totalFloat)
	}
	if ap.totalSeries != nil {
		t.Fatalf("totalSeries must be nil. got %v", ap.totalSeries)
	}
}

func TestArgInINFPositional(t *testing.T) {
	fn := NewKeepLastValue()
	e := &expr{
		etype: etFunc,
		str:   "keepLastValue",
		args: []*expr{
			{etype: etName, str: "in.*"},
			{etype: etName, str: "INF"},
		},
		namedArgs: nil,
	}
	_, err := newplanFunc(e, fn, Context{from: 0, to: 1000}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	klv := fn.(*FuncKeepLastValue)
	if klv.limit != math.MaxInt64 {
		t.Fatalf("limit should be INF. got %d", klv.limit)
	}
}

func TestArgInSeriesKeyword(t *testing.T) {
	fn := NewAsPercent()
	e := &expr{
		etype: etFunc,
		str:   "perSecond",
		args: []*expr{
			{etype: etName, str: "in.*"},
		},
		namedArgs: map[string]*expr{
			"total": {etype: etName, str: "total.*"},
		},
	}
	_, err := newplanFunc(e, fn, Context{from: 0, to: 1000}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	ap := fn.(*FuncAsPercent)
	if !math.IsNaN(ap.totalFloat) {
		t.Fatalf("totalFloat should be nil. got %f", ap.totalFloat)
	}
	if ap.totalSeries == nil {
		//t.Fatalf("totalSeries must not be nil. got nil") // TODO - consume keyword args that are series
	}
}

func TestArgInIntKeyword(t *testing.T) {
	fn := NewAsPercent()
	e := &expr{
		etype: etFunc,
		str:   "perSecond",
		args: []*expr{
			{etype: etName, str: "in.*"},
		},
		namedArgs: map[string]*expr{
			"total": {etype: etInt, str: "10", int: 10},
		},
	}
	_, err := newplanFunc(e, fn, Context{from: 0, to: 1000}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	ap := fn.(*FuncAsPercent)
	if ap.totalFloat != 10 {
		t.Fatalf("totalFloat should be 10. got %f", ap.totalFloat)
	}
	if ap.totalSeries != nil {
		t.Fatalf("totalSeries must be nil. got %v", ap.totalSeries)
	}
}

// TestOptimizationFlags tests that the optimization (PNGroups and MDP for MDP-optimization) flags are
// set in line with the optimization settings passed to the planner.
func TestOptimizationFlags(t *testing.T) {
	from := uint32(1000)
	to := uint32(2000)
	stable := true

	// note, use the number 1 to mean "a PNGroup". these tests currently don't support multiple PNGroups.
	type testCase struct {
		in      string
		wantReq []Req
	}

	compare := func(i int, opts Optimizations, c testCase, plan Plan, err error) {
		// for simplicity, test cases declare 1 to mean "a PNGroup"
		for i, req := range plan.Reqs {
			if req.PNGroup > 0 {
				plan.Reqs[i].PNGroup = 1
			}
		}

		if diff := cmp.Diff(c.wantReq, plan.Reqs); diff != "" {
			t.Errorf("case %d: %q with opts %+v (-want +got):\n%s", i, c.in, opts, diff)
		}
	}

	cases := []testCase{
		{
			// no transparent aggregation so don't align the data. Though, could be MDP optimized
			"a",
			[]Req{
				NewReq("a", from, to, 0, 0, 800),
			},
		},
		{
			"summarize(a,'1h')", // greedy resolution function. disables MDP optimizations
			[]Req{
				NewReq("a", from, to, 0, 0, 0),
			},
		},
		{
			"sum(a)", // transparent aggregation. enables PN-optimization
			[]Req{
				NewReq("a", from, to, 0, 1, 800),
			},
		},
		{
			"summarize(sum(a),'1h')",
			[]Req{
				NewReq("a", from, to, 0, 1, 0),
			},
		},
		{
			// a will go through some functions that don't matter, then hits a transparent aggregation
			"summarize(sum(perSecond(min(scale(a,1)))),'1h')",
			[]Req{
				NewReq("a", from, to, 0, 1, 0),
			},
		},
		{
			// a is not PN-optimizable due to the opaque aggregation, whereas b is thanks to the transparent aggregation, that they hit first.
			"sum(group(groupByTags(a,'sum','foo'), avg(b)))",
			[]Req{
				NewReq("a", from, to, 0, 0, 800),
				NewReq("b", from, to, 0, 1, 800),
			},
		},
		{
			// a is not PN-optimizable because it doesn't go through a transparent aggregation
			// b is, because it hits a transparent aggregation before it hits the opaque aggregation
			// c is neither PN-optimizable, nor MDP-optimizable, because it hits an interval altering + GR function, before it hits anything else
			"groupByTags(group(groupByTags(a,'sum','tag'), avg(b), avg(summarize(c,'1h'))),'sum','tag2')",
			[]Req{
				NewReq("a", from, to, 0, 0, 800),
				NewReq("b", from, to, 0, 1, 800),
				NewReq("c", from, to, 0, 0, 0),
			},
		},
	}
	for i, c := range cases {
		// make a pristine copy of the data such that we can tweak it for different scenarios
		origWantReqs := make([]Req, len(c.wantReq))
		copy(origWantReqs, c.wantReq)

		// first, try with all optimizations:
		opts := Optimizations{
			PreNormalization: true,
			MDP:              true,
		}
		exprs, err := ParseMany([]string{c.in})
		if err != nil {
			t.Fatal(err)
		}
		plan, err := NewPlan(exprs, from, to, 800, stable, opts)
		if err != nil {
			t.Fatal(err)
		}
		compare(i, opts, c, plan, err)

		// now, disable MDP. This should result simply in disabling all MDP flags on all requests
		opts.MDP = false
		c.wantReq = make([]Req, len(origWantReqs))
		copy(c.wantReq, origWantReqs)
		for j := range c.wantReq {
			c.wantReq[j].MDP = 0
		}

		plan, err = NewPlan(exprs, from, to, 800, stable, opts)
		if err != nil {
			t.Fatal(err)
		}
		compare(i, opts, c, plan, err)

		// now disable (only) PN-optimizations. This should result simply in turning off all PNGroups
		opts.MDP = true
		opts.PreNormalization = false
		c.wantReq = make([]Req, len(origWantReqs))
		copy(c.wantReq, origWantReqs)
		for j := range c.wantReq {
			c.wantReq[j].PNGroup = 0
		}
		plan, err = NewPlan(exprs, from, to, 800, stable, opts)
		if err != nil {
			t.Fatal(err)
		}
		compare(i, opts, c, plan, err)

		// now disable both optimizations at the same time
		opts.MDP = false
		opts.PreNormalization = false
		c.wantReq = make([]Req, len(origWantReqs))
		copy(c.wantReq, origWantReqs)
		for j := range c.wantReq {
			c.wantReq[j].MDP = 0
			c.wantReq[j].PNGroup = 0
		}

		plan, err = NewPlan(exprs, from, to, 800, stable, opts)
		if err != nil {
			t.Fatal(err)
		}
		compare(i, opts, c, plan, err)
	}
}

// TestConsolidateBy tests for a variety of input targets, wether consolidateBy settings are correctly
// propagated down the tree (to fetch requests) and up the tree (to runtime consolidation of the output)
func TestConsolidateBy(t *testing.T) {
	from := uint32(1000)
	to := uint32(2000)
	stable := true
	cases := []struct {
		in     string
		expReq []Req // to verify consolidation settings of fetch requests
		expErr error
		expOut []models.Series // we only use QueryPatt and Consolidator field just to check consolidation settings of output series
	}{
		{
			"a",
			[]Req{
				NewReq("a", from, to, 0, 0, 0),
			},
			nil,
			[]models.Series{
				{QueryPatt: "a", Consolidator: consolidation.Avg},
			},
		},
		{
			// consolidation flows both up and down the tree
			`consolidateBy(a, "sum")`,
			[]Req{
				NewReq("a", from, to, consolidation.Sum, 0, 0),
			},
			nil,
			[]models.Series{
				{QueryPatt: `consolidateBy(a,"sum")`, Consolidator: consolidation.Sum},
			},
		},
		{
			// wrap with regular function -> consolidation goes both up and down
			`scale(consolidateBy(a, "sum"),1)`,
			[]Req{
				NewReq("a", from, to, consolidation.Sum, 0, 0),
			},
			nil,
			[]models.Series{
				{QueryPatt: `scale(consolidateBy(a,"sum"),1)`, Consolidator: consolidation.Sum},
			},
		},
		{
			// wrapping by a special function does not affect fetch consolidation, but resets output consolidation
			`perSecond(consolidateBy(a, "sum"))`,
			[]Req{
				NewReq("a", from, to, consolidation.Sum, 0, 0),
			},
			nil,
			[]models.Series{
				{QueryPatt: `perSecond(consolidateBy(a,"sum"))`, Consolidator: consolidation.None},
			},
		},
		{
			// consolidation setting streams down and up unaffected by scale
			`consolidateBy(scale(a, 1), "sum")`,
			[]Req{
				NewReq("a", from, to, consolidation.Sum, 0, 0),
			},
			nil,
			[]models.Series{
				{QueryPatt: `consolidateBy(scale(a,1),"sum")`, Consolidator: consolidation.Sum},
			},
		},
		{
			// perSecond changes data semantics, fetch consolidation should be reset to default
			`consolidateBy(perSecond(a), "sum")`,
			[]Req{
				NewReq("a", from, to, 0, 0, 0),
			},
			nil,
			[]models.Series{
				{QueryPatt: `consolidateBy(perSecond(a),"sum")`, Consolidator: consolidation.Sum},
			},
		},
		{
			// data should be requested with fetch consolidation min, but runtime consolidation max
			// TODO: I think it can be argued that the max here is only intended for the output, not to the inputs
			`consolidateBy(divideSeries(consolidateBy(a, "min"), b), "max")`,
			[]Req{
				NewReq("a", from, to, consolidation.Min, 0, 0),
				NewReq("b", from, to, consolidation.Max, 0, 0),
			},
			nil,
			[]models.Series{
				{QueryPatt: `consolidateBy(divideSeries(consolidateBy(a,"min"),b),"max")`, Consolidator: consolidation.Max},
			},
		},
		{
			// data should be requested with fetch consolidation min, but runtime consolidation max
			`consolidateBy(sumSeries(consolidateBy(a, "min"), b), "max")`,
			[]Req{
				NewReq("a", from, to, consolidation.Min, 0, 0),
				NewReq("b", from, to, consolidation.Max, 0, 0),
			},
			nil,
			[]models.Series{
				{QueryPatt: `consolidateBy(sumSeries(consolidateBy(a,"min"),b),"max")`, Consolidator: consolidation.Max},
			},
		},
	}

	for i, c := range cases {
		// for the purpose of this test, we assume ParseMany works fine.
		exprs, _ := ParseMany([]string{c.in})
		plan, err := NewPlan(exprs, from, to, 800, stable, Optimizations{})
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(err, c.expErr) {
			t.Errorf("case %d: %q, expected error %v - got %v", i, c.in, c.expErr, err)
		}
		if diff := cmp.Diff(c.expReq, plan.Reqs); diff != "" {
			t.Errorf("case %d: %q (-want +got):\n%s", i, c.in, diff)
		}
		dataMap := DataMap{
			NewReq("a", from, to, 0, 0, 0): {{
				QueryPatt:    "a",
				Target:       "a",
				Consolidator: consolidation.Avg, // emulate the fact that a by default will use avg
				Interval:     10,
			}},
			NewReq("a", from, to, consolidation.Min, 0, 0): {{
				QueryPatt:    "a",
				Target:       "a",
				Consolidator: consolidation.Min,
				Interval:     10,
			}},
			NewReq("a", from, to, consolidation.Sum, 0, 0): {{
				QueryPatt:    "a",
				Target:       "a",
				Consolidator: consolidation.Sum,
				Interval:     10,
			}},
			NewReq("b", from, to, consolidation.Max, 0, 0): {{
				QueryPatt:    "b",
				Target:       "b",
				Consolidator: consolidation.Max,
				Interval:     10,
			}},
		}
		out, err := plan.Run(dataMap)
		if err != nil {
			t.Fatal(err)
		}
		if len(out) != len(c.expOut) {
			t.Errorf("case %d: %q, expected %d series output, not %d", i, c.in, len(c.expOut), len(out))
		} else {
			for j, exp := range c.expOut {
				if exp.QueryPatt != out[j].QueryPatt || exp.Consolidator != out[j].Consolidator {
					t.Errorf("case %d: %q, output series mismatch at pos %d: expected %v-%v - got %v-%v", i, c.in, j, exp.QueryPatt, exp.Consolidator, out[j].QueryPatt, out[j].Consolidator)
				}
			}
		}
	}
}

// TestNamingChains tests whether series names (targets) are correct, after a processing chain of multiple functions
func TestNamingChains(t *testing.T) {
	from := uint32(1000)
	to := uint32(2000)
	stable := true
	cases := []struct {
		target string // target request "from user"
		keys   []string
		expOut []string
	}{
		// the first two are cases that we've seen in the wild
		// see https://github.com/grafana/metrictank/issues/648
		{
			`aliasSub(perSecond(metrictank.stats.*.*.input.*.metric_invalid.counter32),'.*\.([^\.]+)\.metric_invalid.*', '\1 metric invalid')`,
			[]string{
				"metrictank.stats.env.instance1.input.carbon.metric_invalid.counter32",
				"metrictank.stats.env.instance2.input.carbon.metric_invalid.counter32",
				"metrictank.stats.env.instance1.input.kafka.metric_invalid.counter32",
			},
			[]string{
				"carbon metric invalid",
				"carbon metric invalid",
				"kafka metric invalid",
			},
		},
		{
			`aliasSub(sumSeries(perSecond(metrictank.stats.*.*.input.*.metric_invalid.counter32)),'.*\.([^\.]+)\.metric_invalid.*', '\1 metric invalid')`,
			[]string{
				"metrictank.stats.env.instance1.input.carbon.metric_invalid.counter32",
				"metrictank.stats.env.instance2.input.carbon.metric_invalid.counter32",
				"metrictank.stats.env.instance1.input.kafka.metric_invalid.counter32",
			},
			[]string{
				"* metric invalid",
			},
		},
		{
			`aliasSub(sumSeries(perSecond(metrictank.stats.*.*.input.*.metric_invalid.counter32)),'.*\.([^\.]+)\.metric_invalid.*', '\1 metric invalid')`,
			[]string{
				"metrictank.stats.env.instance1.input.carbon.metric_invalid.counter32",
			},
			[]string{
				"* metric invalid", // could be argued that "carbon metric invalid" is more useful, but is less consistent. see #648
			},
		},
		// what follows here is a simplified, but comparable test case, for other alias functions
		{
			`aliasByNode(perSecond(*.bar), 0)`,
			[]string{
				"a.bar",
				"b.bar",
			},
			[]string{
				"a",
				"b",
			},
		},
		{
			`aliasByNode(avg(perSecond(*.bar)), 0)`,
			[]string{
				"a.bar",
				"b.bar",
			},
			[]string{
				"*",
			},
		},
		{
			`aliasByNode(avg(perSecond(*.bar)), 0)`,
			[]string{
				"a.bar",
			},
			[]string{
				"*", // "a" could be more useful but see #648
			},
		},
		{
			`aliasByNode(avg(perSecond(seriesByTag('name=~.*.bar'))), 0)`,
			[]string{
				"a.bar;key1=val1",
			},
			[]string{
				"a",
			},
		},
		{
			`alias(perSecond(*.bar), 'a')`,
			[]string{
				"a.bar",
				"b.bar",
			},
			[]string{
				"a",
				"a",
			},
		},
		{
			`alias(avg(perSecond(*.bar)), 'a')`,
			[]string{
				"a",
				"b",
			},
			[]string{
				"a",
			},
		},
		{
			`alias(avg(perSecond(*.bar)), 'a')`,
			[]string{
				"a",
			},
			[]string{
				"a",
			},
		},
	}

	for i, c := range cases {
		exprs, err := ParseMany([]string{c.target})
		if err != nil {
			t.Fatal(err)
		}
		plan, err := NewPlan(exprs, from, to, 800, stable, Optimizations{})
		if err != nil {
			t.Fatal(err)
		}

		// create input data
		series := make([]models.Series, len(c.keys))
		for j, key := range c.keys {
			series[j] = models.Series{
				QueryPatt: plan.Reqs[0].Query,
				Target:    key,
				Interval:  10,
			}
		}
		dataMap := DataMap{
			plan.Reqs[0]: series,
		}
		out, err := plan.Run(dataMap)
		if err != nil {
			t.Fatal(err)
		}
		if len(out) != len(c.expOut) {
			t.Errorf("case %d:\n%q with %d inputs:\nexpected %d series output, not %d", i, c.target, len(c.keys), len(c.expOut), len(out))
		} else {
			for j, exp := range c.expOut {
				if out[j].Target != exp {
					t.Errorf("case %d:\n%q with %d inputs:\noutput series mismatch at pos %d:\nexp: %v\ngot: %v", i, c.target, len(c.keys), j, exp, out[j].Target)
				}
			}
		}
	}
}

// TestParseErrors tests that the proper error is returned for various parse failures
func TestTargetErrors(t *testing.T) {
	from := uint32(1000)
	to := uint32(2000)
	stable := true

	cases := []struct {
		testDescription    string
		target             string // target request "from user"
		expectedParseError error
		expectedPlanError  error
	}{
		{
			"No error (sanity test)",
			`groupByTags(seriesByTag('name=val'),"sum", "tag1", "tag2", "tag3")`,
			nil,
			nil,
		},
		{
			"ArgStrings - Missing args",
			`groupByTags(seriesByTag('name=val'),"sum")`,
			nil,
			ErrMissingArg,
		},
		{
			"ArgStrings - Leftover args",
			`groupByTags(seriesByTag('name=val'),"sum", "tag1", 1)`,
			nil,
			ErrTooManyArg,
		},
		{
			"ArgStrings - Wrong type",
			`groupByTags(seriesByTag('name=val'),"sum", 1)`,
			nil,
			ErrTooManyArg,
		},
		{
			"groupByTags - invalid agg function",
			`groupByTags(seriesByTag('name=val'),"bogus", "tag1")`,
			nil,
			ErrInvalidAggFunc,
		},
		{
			"aliasByTags - all strings",
			`aliasByTags(seriesByTag('name=val'), "name", "tag1")`,
			nil,
			nil,
		},
		{
			"aliasByTags - all ints",
			`aliasByTags(seriesByTag('name=val'), 0, 1)`,
			nil,
			nil,
		},
		{
			"aliasByTags - mixed",
			`aliasByTags(seriesByTag('name=val'), "name", 1)`,
			nil,
			nil,
		},
		{
			"aliasByTags - some unsupported types",
			`aliasByTags(seriesByTag('name=val'), "name", 1.23)`,
			nil,
			ErrTooManyArg,
		},
	}

	for _, c := range cases {
		exprs, err := ParseMany([]string{c.target})
		if err != c.expectedParseError {
			t.Fatalf("case %q: expected parse error %q but got %q", c.testDescription, c.expectedParseError, err)
		}
		_, err = NewPlan(exprs, from, to, 800, stable, Optimizations{})
		if !errors.Is(err, c.expectedPlanError) {
			t.Fatalf("case %q: expected plan error %q to be in the error chain but %q does not contain it", c.testDescription, c.expectedPlanError, err)
		}
	}
}
