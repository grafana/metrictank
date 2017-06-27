package expr

import (
	"math"
	"reflect"
	"testing"

	"github.com/raintank/metrictank/api/models"
	"github.com/raintank/metrictank/consolidation"
)

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
		expErr    error
	}{
		{
			"2 args normal, 0 optional",
			[]*expr{
				{etype: etName, str: "foo.bar.*"},
				{etype: etString, str: "1hour"},
			},
			nil,
			[]Req{
				NewReq("foo.bar.*", from, to, 0),
			},
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
				NewReq("foo.bar.*", from, to, 0),
			},
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
				NewReq("foo.bar.*", from, to, 0),
			},
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
				NewReq("foo.bar.*", from, to, 0),
			},
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
				NewReq("foo.bar.*", from, to, 0),
			},
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
				NewReq("foo.bar.*", from, to, 0),
			},
			nil,
		},
		{
			"missing required argument",
			[]*expr{
				{etype: etName, str: "foo.bar.*"},
			},
			nil,
			nil,
			ErrMissingArg,
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
		if !reflect.DeepEqual(err, c.expErr) {
			t.Errorf("case %d: %q, expected error %v - got %v", i, c.name, c.expErr, err)
		}
		if !reflect.DeepEqual(req, c.expReq) {
			t.Errorf("case %d: %q, expected req %v - got %v", i, c.name, c.expReq, req)
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
		t.Fatalf("totalSeries must not be nil. got nil")
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
				NewReq("a", from, to, 0),
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
				NewReq("a", from, to, consolidation.Sum),
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
				NewReq("a", from, to, consolidation.Sum),
			},
			nil,
			[]models.Series{
				{QueryPatt: `scale(consolidateBy(a,"sum"),1.000000)`, Consolidator: consolidation.Sum},
			},
		},
		{
			// wrapping by a special function does not affect fetch consolidation, but resets output consolidation
			`perSecond(consolidateBy(a, "sum"))`,
			[]Req{
				NewReq("a", from, to, consolidation.Sum),
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
				NewReq("a", from, to, consolidation.Sum),
			},
			nil,
			[]models.Series{
				{QueryPatt: `consolidateBy(scale(a,1.000000),"sum")`, Consolidator: consolidation.Sum},
			},
		},
		{
			// perSecond changes data semantics, fetch consolidation should be reset to default
			`consolidateBy(perSecond(a), "sum")`,
			[]Req{
				NewReq("a", from, to, 0),
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
				NewReq("a", from, to, consolidation.Min),
				NewReq("b", from, to, consolidation.Max),
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
				NewReq("a", from, to, consolidation.Min),
				NewReq("b", from, to, consolidation.Max),
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
		plan, err := NewPlan(exprs, from, to, 800, stable, nil)
		if err != nil {
			t.Fatal(err)
		}
		if !reflect.DeepEqual(err, c.expErr) {
			t.Errorf("case %d: %q, expected error %v - got %v", i, c.in, c.expErr, err)
		}
		if !reflect.DeepEqual(plan.Reqs, c.expReq) {
			t.Errorf("case %d: %q, expected req %v - got %v", i, c.in, c.expReq, plan.Reqs)
		}
		input := map[Req][]models.Series{
			NewReq("a", from, to, 0): {{
				QueryPatt:    "a",
				Consolidator: consolidation.Avg, // emulate the fact that a by default will use avg
			}},
			NewReq("a", from, to, consolidation.Min): {{
				QueryPatt:    "a",
				Consolidator: consolidation.Min,
			}},
			NewReq("a", from, to, consolidation.Sum): {{
				QueryPatt:    "a",
				Consolidator: consolidation.Sum,
			}},
			NewReq("b", from, to, consolidation.Max): {{
				QueryPatt:    "b",
				Consolidator: consolidation.Max,
			}},
		}
		out, err := plan.Run(input)
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
		// see https://github.com/raintank/metrictank/issues/648
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
		plan, err := NewPlan(exprs, from, to, 800, stable, nil)
		if err != nil {
			t.Fatal(err)
		}

		// create input data
		series := make([]models.Series, len(c.keys))
		for j, key := range c.keys {
			series[j] = models.Series{
				QueryPatt: plan.Reqs[0].Query,
				Target:    key,
			}
		}
		input := map[Req][]models.Series{
			plan.Reqs[0]: series,
		}
		out, err := plan.Run(input)
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
