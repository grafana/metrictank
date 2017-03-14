package expr

import (
	"fmt"
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
			&expr{target: "metric"},
		},
		{
			"metric.foo",
			&expr{target: "metric.foo"},
		},
		{"metric.*.foo",
			&expr{target: "metric.*.foo"},
		},
		{
			"func(metric)",
			&expr{
				target:    "func",
				etype:     etFunc,
				args:      []*expr{{target: "metric"}},
				argString: "metric",
			},
		},
		{
			"func(metric1,metric2,metric3)",
			&expr{
				target: "func",
				etype:  etFunc,
				args: []*expr{
					{target: "metric1"},
					{target: "metric2"},
					{target: "metric3"}},
				argString: "metric1,metric2,metric3",
			},
		},
		{
			"func1(metric1,func2(metricA, metricB),metric3)",
			&expr{
				target: "func1",
				etype:  etFunc,
				args: []*expr{
					{target: "metric1"},
					{target: "func2",
						etype:     etFunc,
						args:      []*expr{{target: "metricA"}, {target: "metricB"}},
						argString: "metricA, metricB",
					},
					{target: "metric3"}},
				argString: "metric1,func2(metricA, metricB),metric3",
			},
		},

		{
			"3",
			&expr{val: 3, etype: etConst},
		},
		{
			"3.1",
			&expr{val: 3.1, etype: etConst},
		},
		{
			"func1(metric1, 3, 1e2, 2e-3)",
			&expr{
				target: "func1",
				etype:  etFunc,
				args: []*expr{
					{target: "metric1"},
					{val: 3, etype: etConst},
					{val: 100, etype: etConst},
					{val: 0.002, etype: etConst},
				},
				argString: "metric1, 3, 1e2, 2e-3",
			},
		},
		{
			"func1(metric1, 'stringconst')",
			&expr{
				target: "func1",
				etype:  etFunc,
				args: []*expr{
					{target: "metric1"},
					{valStr: "stringconst", etype: etString},
				},
				argString: "metric1, 'stringconst'",
			},
		},
		{
			`func1(metric1, "stringconst")`,
			&expr{
				target: "func1",
				etype:  etFunc,
				args: []*expr{
					{target: "metric1"},
					{valStr: "stringconst", etype: etString},
				},
				argString: `metric1, "stringconst"`,
			},
		},
		{
			"func1(metric1, -3)",
			&expr{
				target: "func1",
				etype:  etFunc,
				args: []*expr{
					{target: "metric1"},
					{val: -3, etype: etConst},
				},
				argString: "metric1, -3",
			},
		},

		{
			"func1(metric1, -3 , 'foo' )",
			&expr{
				target: "func1",
				etype:  etFunc,
				args: []*expr{
					{target: "metric1"},
					{val: -3, etype: etConst},
					{valStr: "foo", etype: etString},
				},
				argString: "metric1, -3 , 'foo' ",
			},
		},

		{
			"func(metric, key='value')",
			&expr{
				target: "func",
				etype:  etFunc,
				args: []*expr{
					{target: "metric"},
				},
				namedArgs: map[string]*expr{
					"key": {etype: etString, valStr: "value"},
				},
				argString: "metric, key='value'",
			},
		},
		{
			"func(metric, key=true)",
			&expr{
				target: "func",
				etype:  etFunc,
				args: []*expr{
					{target: "metric"},
				},
				namedArgs: map[string]*expr{
					"key": {etype: etName, target: "true"},
				},
				argString: "metric, key=true",
			},
		},
		{
			"func(metric, key=1)",
			&expr{
				target: "func",
				etype:  etFunc,
				args: []*expr{
					{target: "metric"},
				},
				namedArgs: map[string]*expr{
					"key": {etype: etConst, val: 1},
				},
				argString: "metric, key=1",
			},
		},
		{
			"func(metric, key=0.1)",
			&expr{
				target: "func",
				etype:  etFunc,
				args: []*expr{
					{target: "metric"},
				},
				namedArgs: map[string]*expr{
					"key": {etype: etConst, val: 0.1},
				},
				argString: "metric, key=0.1",
			},
		},

		{
			"func(metric, 1, key='value')",
			&expr{
				target: "func",
				etype:  etFunc,
				args: []*expr{
					{target: "metric"},
					{etype: etConst, val: 1},
				},
				namedArgs: map[string]*expr{
					"key": {etype: etString, valStr: "value"},
				},
				argString: "metric, 1, key='value'",
			},
		},
		{
			"func(metric, key='value', 1)",
			&expr{
				target: "func",
				etype:  etFunc,
				args: []*expr{
					{target: "metric"},
					{etype: etConst, val: 1},
				},
				namedArgs: map[string]*expr{
					"key": {etype: etString, valStr: "value"},
				},
				argString: "metric, key='value', 1",
			},
		},
		{
			"func(metric, key1='value1', key2='value two is here')",
			&expr{
				target: "func",
				etype:  etFunc,
				args: []*expr{
					{target: "metric"},
				},
				namedArgs: map[string]*expr{
					"key1": {etype: etString, valStr: "value1"},
					"key2": {etype: etString, valStr: "value two is here"},
				},
				argString: "metric, key1='value1', key2='value two is here'",
			},
		},
		{
			"func(metric, key2='value2', key1='value1')",
			&expr{
				target: "func",
				etype:  etFunc,
				args: []*expr{
					{target: "metric"},
				},
				namedArgs: map[string]*expr{
					"key2": {etype: etString, valStr: "value2"},
					"key1": {etype: etString, valStr: "value1"},
				},
				argString: "metric, key2='value2', key1='value1'",
			},
		},

		{
			`foo.{bar,baz}.qux`,
			&expr{
				target: "foo.{bar,baz}.qux",
				etype:  etName,
			},
		},
		{
			`foo.b[0-9].qux`,
			&expr{
				target: "foo.b[0-9].qux",
				etype:  etName,
			},
		},
		{
			`virt.v1.*.text-match:<foo.bar.qux>`,
			&expr{
				target: "virt.v1.*.text-match:<foo.bar.qux>",
				etype:  etName,
			},
		},
	}

	for _, tt := range tests {
		fmt.Println("======================", tt.s, "=================")
		e, _, err := Parse(tt.s)
		fmt.Println("=======")
		spew.Dump(e)
		if err != nil {
			t.Errorf("parse for %+v failed: err=%v", tt.s, err)
			continue
		}
		if !reflect.DeepEqual(e, tt.e) {
			t.Errorf("parse for %+v failed:\ngot  %+s\nwant %+v", tt.s, spew.Sdump(e), spew.Sdump(tt.e))
		}
	}
}
