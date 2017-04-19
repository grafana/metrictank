package expr

import (
	"reflect"
	"testing"

	"github.com/davecgh/go-spew/spew"
)

func TestParse(t *testing.T) {

	tests := []struct {
		s string
		e *expr
	}{
		{"metric",
			&expr{str: "metric"},
		},
		{
			"metric.foo",
			&expr{str: "metric.foo"},
		},
		{"metric.*.foo",
			&expr{str: "metric.*.foo"},
		},
		{
			"func(metric)",
			&expr{
				str:     "func",
				etype:   etFunc,
				args:    []*expr{{str: "metric"}},
				argsStr: "metric",
			},
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
		},

		{
			"3",
			&expr{float: 3, str: "3", etype: etConst},
		},
		{
			"3.1",
			&expr{float: 3.1, str: "3.1", etype: etConst},
		},
		{
			"func1(metric1, 3, 1e2, 2e-3)",
			&expr{
				str:   "func1",
				etype: etFunc,
				args: []*expr{
					{str: "metric1"},
					{float: 3, str: "3", etype: etConst},
					{float: 100, str: "1e2", etype: etConst},
					{float: 0.002, str: "2e-3", etype: etConst},
				},
				argsStr: "metric1, 3, 1e2, 2e-3",
			},
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
		},
		{
			"func1(metric1, -3)",
			&expr{
				str:   "func1",
				etype: etFunc,
				args: []*expr{
					{str: "metric1"},
					{float: -3, str: "-3", etype: etConst},
				},
				argsStr: "metric1, -3",
			},
		},

		{
			"func1(metric1, -3 , 'foo' )",
			&expr{
				str:   "func1",
				etype: etFunc,
				args: []*expr{
					{str: "metric1"},
					{float: -3, str: "-3", etype: etConst},
					{str: "foo", etype: etString},
				},
				argsStr: "metric1, -3 , 'foo' ",
			},
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
					"key": {etype: etName, str: "true"},
				},
				argsStr: "metric, key=true",
			},
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
					"key": {etype: etConst, str: "1", float: 1},
				},
				argsStr: "metric, key=1",
			},
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
					"key": {etype: etConst, str: "0.1", float: 0.1},
				},
				argsStr: "metric, key=0.1",
			},
		},

		{
			"func(metric, 1, key='value')",
			&expr{
				str:   "func",
				etype: etFunc,
				args: []*expr{
					{str: "metric"},
					{etype: etConst, str: "1", float: 1},
				},
				namedArgs: map[string]*expr{
					"key": {etype: etString, str: "value"},
				},
				argsStr: "metric, 1, key='value'",
			},
		},
		{
			"func(metric, key='value', 1)",
			&expr{
				str:   "func",
				etype: etFunc,
				args: []*expr{
					{str: "metric"},
					{etype: etConst, str: "1", float: 1},
				},
				namedArgs: map[string]*expr{
					"key": {etype: etString, str: "value"},
				},
				argsStr: "metric, key='value', 1",
			},
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
		},

		{
			`foo.{bar,baz}.qux`,
			&expr{
				str:   "foo.{bar,baz}.qux",
				etype: etName,
			},
		},
		{
			`foo.b[0-9].qux`,
			&expr{
				str:   "foo.b[0-9].qux",
				etype: etName,
			},
		},
		{
			`virt.v1.*.text-match:<foo.bar.qux>`,
			&expr{
				str:   "virt.v1.*.text-match:<foo.bar.qux>",
				etype: etName,
			},
		},
	}

	for _, tt := range tests {
		e, _, err := Parse(tt.s)
		if err != nil {
			t.Errorf("parse for %+v failed: err=%v", tt.s, err)
			continue
		}
		if !reflect.DeepEqual(e, tt.e) {
			t.Errorf("parse for %+v failed:\ngot  %+s\nwant %+v", tt.s, spew.Sdump(e), spew.Sdump(tt.e))
		}
	}
}
