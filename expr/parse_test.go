package expr

import (
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
	"github.com/sergi/go-diff/diffmatchpatch"
)

func TestParse(t *testing.T) {

	tests := []struct {
		s   string
		e   *expr
		err error
	}{
		{"metric",
			&expr{str: "metric"},
			nil,
		},
		{
			"metric.foo",
			&expr{str: "metric.foo"},
			nil,
		},
		{"metric.*.foo",
			&expr{str: "metric.*.foo"},
			nil,
		},
		{
			"a.b.c;tagkey1=tagvalue1;tagkey2=tagvalue2",
			&expr{str: "a.b.c;tagkey1=tagvalue1;tagkey2=tagvalue2"},
			nil,
		},
		{
			"func(metric;tag1=value1, key='value')",
			&expr{
				str:     "func",
				etype:   etFunc,
				args:    []*expr{{str: "metric;tag1=value1"}},
				argsStr: "metric;tag1=value1, key='value'",
				namedArgs: map[string]*expr{
					"key": {etype: etString, str: "value"},
				},
			},
			nil,
		},
		{
			"func(metric)",
			&expr{
				str:     "func",
				etype:   etFunc,
				args:    []*expr{{str: "metric"}},
				argsStr: "metric",
			},
			nil,
		},
		{
			"func(metric1,metric2,metric3)",
			&expr{
				str:   "func",
				etype: etFunc,
				args: []*expr{
					{str: "metric1"},
					{str: "metric2"},
					{str: "metric3"}},
				argsStr: "metric1,metric2,metric3",
			},
			nil,
		},
		{
			"func1(metric1,func2(metricA, metricB),metric3)",
			&expr{
				str:   "func1",
				etype: etFunc,
				args: []*expr{
					{str: "metric1"},
					{str: "func2",
						etype:   etFunc,
						args:    []*expr{{str: "metricA"}, {str: "metricB"}},
						argsStr: "metricA, metricB",
					},
					{str: "metric3"}},
				argsStr: "metric1,func2(metricA, metricB),metric3",
			},
			nil,
		},

		{
			"3",
			&expr{int: 3, str: "3", etype: etInt},
			nil,
		},
		{
			"3.1",
			&expr{float: 3.1, str: "3.1", etype: etFloat},
			nil,
		},
		{
			"func1(metric1, 3, 1e2, 2e-3)",
			&expr{
				str:   "func1",
				etype: etFunc,
				args: []*expr{
					{str: "metric1"},
					{int: 3, str: "3", etype: etInt},
					{float: 100, str: "1e2", etype: etFloat},
					{float: 0.002, str: "2e-3", etype: etFloat},
				},
				argsStr: "metric1, 3, 1e2, 2e-3",
			},
			nil,
		},
		{
			"func1(metric1, 'stringconst')",
			&expr{
				str:   "func1",
				etype: etFunc,
				args: []*expr{
					{str: "metric1"},
					{str: "stringconst", etype: etString},
				},
				argsStr: "metric1, 'stringconst'",
			},
			nil,
		},
		{
			`func1(metric1, "stringconst")`,
			&expr{
				str:   "func1",
				etype: etFunc,
				args: []*expr{
					{str: "metric1"},
					{str: "stringconst", etype: etString},
				},
				argsStr: `metric1, "stringconst"`,
			},
			nil,
		},
		{
			"func1(metric1, -3)",
			&expr{
				str:   "func1",
				etype: etFunc,
				args: []*expr{
					{str: "metric1"},
					{int: -3, str: "-3", etype: etInt},
				},
				argsStr: "metric1, -3",
			},
			nil,
		},

		{
			"func1(metric1, -3 , 'foo' )",
			&expr{
				str:   "func1",
				etype: etFunc,
				args: []*expr{
					{str: "metric1"},
					{int: -3, str: "-3", etype: etInt},
					{str: "foo", etype: etString},
				},
				argsStr: "metric1, -3 , 'foo' ",
			},
			nil,
		},

		{
			"func(metric, key='value')",
			&expr{
				str:   "func",
				etype: etFunc,
				args: []*expr{
					{str: "metric"},
				},
				namedArgs: map[string]*expr{
					"key": {etype: etString, str: "value"},
				},
				argsStr: "metric, key='value'",
			},
			nil,
		},
		{
			"func(metric, key=true)",
			&expr{
				str:   "func",
				etype: etFunc,
				args: []*expr{
					{str: "metric"},
				},
				namedArgs: map[string]*expr{
					"key": {etype: etBool, str: "true", bool: true},
				},
				argsStr: "metric, key=true",
			},
			nil,
		},
		{
			"func(metric, False)",
			&expr{
				str:   "func",
				etype: etFunc,
				args: []*expr{
					{str: "metric"},
					{etype: etBool, str: "False", bool: false},
				},
				argsStr: "metric, False",
			},
			nil,
		},
		{
			"func(metric, key=1)",
			&expr{
				str:   "func",
				etype: etFunc,
				args: []*expr{
					{str: "metric"},
				},
				namedArgs: map[string]*expr{
					"key": {etype: etInt, str: "1", int: 1},
				},
				argsStr: "metric, key=1",
			},
			nil,
		},
		{
			"func(metric, key=0.1)",
			&expr{
				str:   "func",
				etype: etFunc,
				args: []*expr{
					{str: "metric"},
				},
				namedArgs: map[string]*expr{
					"key": {etype: etFloat, str: "0.1", float: 0.1},
				},
				argsStr: "metric, key=0.1",
			},
			nil,
		},

		{
			"func(metric, 1, key='value')",
			&expr{
				str:   "func",
				etype: etFunc,
				args: []*expr{
					{str: "metric"},
					{etype: etInt, str: "1", int: 1},
				},
				namedArgs: map[string]*expr{
					"key": {etype: etString, str: "value"},
				},
				argsStr: "metric, 1, key='value'",
			},
			nil,
		},
		{
			"func(metric, key='value', 1)",
			&expr{
				str:   "func",
				etype: etFunc,
				args: []*expr{
					{str: "metric"},
					{etype: etInt, str: "1", int: 1},
				},
				namedArgs: map[string]*expr{
					"key": {etype: etString, str: "value"},
				},
				argsStr: "metric, key='value', 1",
			},
			nil,
		},
		{
			"func(metric, key1='value1', key2='value two is here')",
			&expr{
				str:   "func",
				etype: etFunc,
				args: []*expr{
					{str: "metric"},
				},
				namedArgs: map[string]*expr{
					"key1": {etype: etString, str: "value1"},
					"key2": {etype: etString, str: "value two is here"},
				},
				argsStr: "metric, key1='value1', key2='value two is here'",
			},
			nil,
		},
		{
			"func(metric, key2='value2', key1='value1')",
			&expr{
				str:   "func",
				etype: etFunc,
				args: []*expr{
					{str: "metric"},
				},
				namedArgs: map[string]*expr{
					"key2": {etype: etString, str: "value2"},
					"key1": {etype: etString, str: "value1"},
				},
				argsStr: "metric, key2='value2', key1='value1'",
			},
			nil,
		},
		{
			"func(metric1;tag1=val1, key2='value2', metric2;tag2=val2, key1='value1')",
			&expr{
				str:   "func",
				etype: etFunc,
				args: []*expr{
					{str: "metric1;tag1=val1"},
					{str: "metric2;tag2=val2"},
				},
				namedArgs: map[string]*expr{
					"key2": {etype: etString, str: "value2"},
					"key1": {etype: etString, str: "value1"},
				},
				argsStr: "metric1;tag1=val1, key2='value2', metric2;tag2=val2, key1='value1'",
			},
			nil,
		},

		{
			`foo.{bar,baz}.qux`,
			&expr{
				str:   "foo.{bar,baz}.qux",
				etype: etName,
			},
			nil,
		},
		{
			`foo.b[0-9].qux`,
			&expr{
				str:   "foo.b[0-9].qux",
				etype: etName,
			},
			nil,
		},
		{
			`virt.v1.*.text-match:<foo.bar.qux>`,
			&expr{
				str:   "virt.v1.*.text-match:<foo.bar.qux>",
				etype: etName,
			},
			nil,
		},
		{
			`foo.()`,
			nil,
			ErrIllegalCharacter,
		},
		{
			`foo.*()`,
			nil,
			ErrIllegalCharacter,
		},
		{
			`foo.{bar,baz}.qux()`,
			nil,
			ErrIllegalCharacter,
		},
	}

	for _, tt := range tests {
		e, _, err := Parse(tt.s)
		if err != tt.err {
			t.Errorf("case %+v expected err %v, got %v", tt.s, tt.err, err)
			continue
		}
		if !reflect.DeepEqual(e, tt.e) {
			spew.Config.DisablePointerAddresses = true
			exp := spew.Sdump(tt.e)
			got := spew.Sdump(e)
			spew.Config.DisablePointerAddresses = false
			dmp := diffmatchpatch.New()
			diffs := dmp.DiffMain(exp, got, false)
			format := `##### case %+v #####
### expected ###
%+v

### got ###
%s

###diff ###
%s`
			t.Errorf(format, tt.s, exp, got, dmp.DiffPrettyText(diffs))
		}

	}
}

func TestExtractMetric(t *testing.T) {
	var tests = []struct {
		in  string
		out string
	}{
		{
			"foo",
			"foo",
		},
		{
			"perSecond(foo)",
			"foo",
		},
		{
			"foo.bar",
			"foo.bar",
		},
		{
			"perSecond(foo.bar",
			"foo.bar",
		},
		{
			"movingAverage(foo.bar,10)",
			"foo.bar",
		},
		{
			"scale(scaleToSeconds(nonNegativeDerivative(foo.bar),60),60)",
			"foo.bar",
		},
		{
			"divideSeries(foo.bar,baz.quux)",
			"foo.bar",
		},
	}

	for _, tt := range tests {
		if m := extractMetric(tt.in); m != tt.out {
			t.Errorf("extractMetric(%q)=%q, want %q", tt.in, m, tt.out)
		}
	}
}
