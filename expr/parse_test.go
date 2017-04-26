package expr

import (
	"reflect"
	"testing"
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
			&expr{float: 3, str: "3", etype: etConst},
			nil,
		},
		{
			"3.1",
			&expr{float: 3.1, str: "3.1", etype: etConst},
			nil,
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
					{float: -3, str: "-3", etype: etConst},
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
					{float: -3, str: "-3", etype: etConst},
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
					"key": {etype: etBool, str: "true", b: true},
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
					{etype: etBool, str: "False", b: false},
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
					"key": {etype: etConst, str: "1", float: 1},
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
					"key": {etype: etConst, str: "0.1", float: 0.1},
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
					{etype: etConst, str: "1", float: 1},
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
					{etype: etConst, str: "1", float: 1},
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
			t.Errorf("case %+v\nexp %+v\ngot %+s", tt.s, tt.e, e)
		}
	}
}
