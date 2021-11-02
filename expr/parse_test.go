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
		leftover string
	}{
		{
			"metric",
			&expr{str: "metric"},
			nil,
			"",
		},
		{
			"metric.foo",
			&expr{str: "metric.foo"},
			nil,
			"",
		},
		{
			"metric.*.foo",
			&expr{str: "metric.*.foo"},
			nil,
			"",
		},
		{
			"metric.*.foo ",
			&expr{str: "metric.*.foo"},
			nil,
			"",
		},
		{
			"metric.*.foo  ",
			&expr{str: "metric.*.foo"},
			nil,
			"",
		},
		{
			"a.b.c;tagkey1=tagvalue1;tagkey2=tagvalue2",
			&expr{str: "a.b.c;tagkey1=tagvalue1;tagkey2=tagvalue2"},
			nil,
			"",
		},
		{
			"a.b.c;tagkey1=tagvalue1;tagkey2=tagvalue2  ",
			&expr{str: "a.b.c;tagkey1=tagvalue1;tagkey2=tagvalue2"},
			nil,
			"",
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
			"",
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
			"",
		},
		{
			"func(metric",
			&expr{
				str:   "func",
				etype: etFunc,
			},
			ErrIncompleteCall,
			"",
		},
		{
			"func(metric,)",
			&expr{
				str:   "func",
				etype: etFunc,
			},
			ErrMissingArg,
			"",
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
			"",
		},
		{
			"func1(metric1,func2(func3(func4(metricA,'foo'))))",
			&expr{
				str:   "func1",
				etype: etFunc,
				args: []*expr{
					{str: "metric1"},
					{str: "func2",
						etype: etFunc,
						args: []*expr{
							{str: "func3",
								etype: etFunc,
								args: []*expr{
									{str: "func4",
										etype:   etFunc,
										args:    []*expr{{str: "metricA"}, {etype: etString, str: "foo"}},
										argsStr: "metricA,'foo'",
									},
								},
								argsStr: "func4(metricA,'foo')",
							},
						},
						argsStr: "func3(func4(metricA,'foo'))",
					},
				},
				argsStr: "metric1,func2(func3(func4(metricA,'foo')))",
			},
			nil,
			"",
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
			"",
		},
		{
			"3",
			&expr{str: "3"},
			nil,
			"",
		},
		{
			"3.1",
			&expr{str: "3.1"},
			nil,
			"",
		},
		{
			"3  ",
			&expr{str: "3"},
			nil,
			"",
		},
		{
			"3a",
			&expr{str: "3a"},
			nil,
			"",
		},
		{
			"3a ",
			&expr{str: "3a"},
			nil,
			"",
		},
		{
			"3.0a.b.c",
			&expr{str: "3.0a.b.c"},
			nil,
			"",
		},
		{
			"a3",
			&expr{str: "a3"},
			nil,
			"",
		},
		{
			"func1(1  , a)",
			&expr{
				str:   "func1",
				etype: etFunc,
				args: []*expr{
					{int: 1, str: "1", etype: etInt},
					{str: "a"},
				},
				argsStr: "1  , a",
			},
			nil,
			"",
		},
		{
			"func1(1, 10a , 2b) ",
			&expr{
				str:   "func1",
				etype: etFunc,
				args: []*expr{
					{int: 1, str: "1", etype: etInt},
					{str: "10a"},
					{str: "2b"},
				},
				argsStr: "1, 10a , 2b",
			},
			nil,
			"",
		},
		{
			"func1(1, func2(10a , 15), 2b) ",
			&expr{
				str:   "func1",
				etype: etFunc,
				args: []*expr{
					{int: 1, str: "1", etype: etInt},
					{
						str: "func2",
						etype: etFunc,
						args: []*expr{
							{str: "10a"},
							{int: 15, str: "15", etype: etInt},
						},
						argsStr: "10a , 15",
					},
					{str: "2b"},
				},
				argsStr: "1, func2(10a , 15), 2b",
			},
			nil,
			"",
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
			"",
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
			"",
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
			"",
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
			"",
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
			"",
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
			"",
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
			"",
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
			"",
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
			"",
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
			"",
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
			"",
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
			"",
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
			"",
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
			"",
		},
		{
			"func(metric1;tag1=val1, key1='value1', metric2;tag2=val2, key2=true, metric3;tag3=val3, key3=None, metric4;tag4=val4)",
			&expr{
				str:   "func",
				etype: etFunc,
				args: []*expr{
					{str: "metric1;tag1=val1"},
					{str: "metric2;tag2=val2"},
					{str: "metric3;tag3=val3"},
					{str: "metric4;tag4=val4"},
				},
				namedArgs: map[string]*expr{
					"key1": {etype: etString, str: "value1"},
					"key2": {etype: etBool, str: "true", bool: true},
					"key3": {etype: etName, str: "None"},
				},
				argsStr: "metric1;tag1=val1, key1='value1', metric2;tag2=val2, key2=true, metric3;tag3=val3, key3=None, metric4;tag4=val4",
			},
			nil,
			"",
		},
		{
			"func(metric, key2='true', key1='false')",
			&expr{
				str:   "func",
				etype: etFunc,
				args: []*expr{
					{str: "metric"},
				},
				namedArgs: map[string]*expr{
					"key2": {etype: etString, str: "true"},
					"key1": {etype: etString, str: "false"},
				},
				argsStr: "metric, key2='true', key1='false'",
			},
			nil,
			"",
		},
		{
			`foo.{bar,baz}.qux`,
			&expr{
				str:   "foo.{bar,baz}.qux",
				etype: etName,
			},
			nil,
			"",
		},
		{
			`foo.b[0-9].qux`,
			&expr{
				str:   "foo.b[0-9].qux",
				etype: etName,
			},
			nil,
			"",
		},
		{
			`virt.v1.*.text-match:<foo.bar.qux>`,
			&expr{
				str:   "virt.v1.*.text-match:<foo.bar.qux>",
				etype: etName,
			},
			nil,
			"",
		},
		{
			`foo.()`,
			nil,
			ErrIllegalCharacter,
			"",
		},
		{
			`foo.*()`,
			nil,
			ErrIllegalCharacter,
			"",
		},
		{
			`foo.{bar,baz}.qux()`,
			nil,
			ErrIllegalCharacter,
			"",
		},
		// PIPE SYNTAX TESTS
		{
			"metric | func()",
			&expr{
				str:     "func",
				etype:   etFunc,
				args:    []*expr{{str: "metric"}},
				argsStr: "",
			},
			nil,
			"",
		},
		{
			"metric | abc",
			&expr{str: "metric"},
			ErrExpectingPipeFunc,
			"",
		},
		{
			"metric | true",
			&expr{str: "metric"},
			ErrExpectingPipeFunc,
			"",
		},
		{
			"metric | 3",
			&expr{str: "metric"},
			ErrExpectingPipeFunc,
			"",
		},
		{
			"metric | func(metric",
			&expr{
				str:   "metric",
				etype: etName,
			},
			ErrIncompleteCall,
			"",
		},
		{
			"metric | func(metric,)",
			&expr{
				str: "metric",
			},
			ErrMissingArg,
			"",
		},
		{
			"metric | func(",
			&expr{
				str: "metric",
			},
			ErrIncompleteCall,
			"",
		},
		{
			"metric;tag1=value1 | func(key='value')",
			&expr{
				str:     "func",
				etype:   etFunc,
				args:    []*expr{{str: "metric;tag1=value1"}},
				argsStr: "key='value'",
				namedArgs: map[string]*expr{
					"key": {etype: etString, str: "value"},
				},
			},
			nil,
			"",
		},
		{
			"metric1|func(metric2,metric3)",
			&expr{
				str:   "func",
				etype: etFunc,
				args: []*expr{
					{str: "metric1"},
					{str: "metric2"},
					{str: "metric3"}},
				argsStr: "metric2,metric3",
			},
			nil,
			"",
		},
		{
			"metric1 |  func(metric2,'stringconst',metric3)",
			&expr{
				str:   "func",
				etype: etFunc,
				args: []*expr{
					{str: "metric1"},
					{str: "metric2"},
					{str: "stringconst", etype: etString},
					{str: "metric3"}},
				argsStr: "metric2,'stringconst',metric3",
			},
			nil,
			"",
		},
		{
			"metric1|func1('stringconst')",
			&expr{
				str:   "func1",
				etype: etFunc,
				args: []*expr{
					{str: "metric1"},
					{str: "stringconst", etype: etString},
				},
				argsStr: "'stringconst'",
			},
			nil,
			"",
		},
		{
			"metric1|func1(-3)",
			&expr{
				str:   "func1",
				etype: etFunc,
				args: []*expr{
					{str: "metric1"},
					{int: -3, str: "-3", etype: etInt},
				},
				argsStr: "-3",
			},
			nil,
			"",
		},
		{
			" metric|func(key2='true', key1='false')",
			&expr{
				str:   "func",
				etype: etFunc,
				args: []*expr{
					{str: "metric"},
				},
				namedArgs: map[string]*expr{
					"key2": {etype: etString, str: "true"},
					"key1": {etype: etString, str: "false"},
				},
				argsStr: "key2='true', key1='false'",
			},
			nil,
			"",
		},
		{
			"metric|func(key1='value1', key2='value two is here')",
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
				argsStr: "key1='value1', key2='value two is here'",
			},
			nil,
			"",
		},
		{
			"metric1;tag1=val1 | func(key1='value1', metric2;tag2=val2, key2=true, metric3;tag3=val3, key3=None, metric4;tag4=val4)",
			&expr{
				str:   "func",
				etype: etFunc,
				args: []*expr{
					{str: "metric1;tag1=val1"},
					{str: "metric2;tag2=val2"},
					{str: "metric3;tag3=val3"},
					{str: "metric4;tag4=val4"},
				},
				namedArgs: map[string]*expr{
					"key1": {etype: etString, str: "value1"},
					"key2": {etype: etBool, str: "true", bool: true},
					"key3": {etype: etName, str: "None"},
				},
				argsStr: "key1='value1', metric2;tag2=val2, key2=true, metric3;tag3=val3, key3=None, metric4;tag4=val4",
			},
			nil,
			"",
		},
		{
			"metric1 | func1() | func2() | func3(3)",
			&expr{
				str:   "func3",
				etype: etFunc,
				args: []*expr{
					{
						str:   "func2",
						etype: etFunc,
						args: []*expr{
							{
								str:   "func1",
								etype: etFunc,
								args: []*expr{
									{str: "metric1"},
								},
								argsStr: "",
							},
						},
						argsStr: "",
					},
					{etype: etInt, str: "3", int: 3},
				},
				argsStr: "3",
			},
			nil,
			"",
		},
		{
			"func1(metric1 | func2(), 3)",
			&expr{
				str:   "func1",
				etype: etFunc,
				args: []*expr{
					{
						str:   "func2",
						etype: etFunc,
						args: []*expr{
							{str: "metric1"},
						},
						argsStr: "",
					},
					{etype: etInt, str: "3", int: 3},
				},
				argsStr: "metric1 | func2(), 3",
			},
			nil,
			"",
		},
		{
			"func1(1 | func2(), 3)",
			&expr{
				str:   "func1",
				etype: etFunc,
				args: []*expr{
					{
						str:   "func2",
						etype: etFunc,
						args: []*expr{
							{str: "1"},
						},
						argsStr: "",
					},
					{etype: etInt, str: "3", int: 3},
				},
				argsStr: "1 | func2(), 3",
			},
			nil,
			"",
		},
		{
			"metric1 | func1(func2(func3(func4(metricA,'foo'))))",
			&expr{
				str:   "func1",
				etype: etFunc,
				args: []*expr{
					{str: "metric1"},
					{str: "func2",
						etype: etFunc,
						args: []*expr{
							{str: "func3",
								etype: etFunc,
								args: []*expr{
									{str: "func4",
										etype:   etFunc,
										args:    []*expr{{str: "metricA"}, {etype: etString, str: "foo"}},
										argsStr: "metricA,'foo'",
									},
								},
								argsStr: "func4(metricA,'foo')",
							},
						},
						argsStr: "func3(func4(metricA,'foo'))",
					},
				},
				argsStr: "func2(func3(func4(metricA,'foo')))",
			},
			nil,
			"",
		},
		{
			"metric1 | func1(func3(func4(metricA,'foo')) | func2())",
			&expr{
				str:   "func1",
				etype: etFunc,
				args: []*expr{
					{str: "metric1"},
					{
						str:   "func2",
						etype: etFunc,
						args: []*expr{
							{
								str:   "func3",
								etype: etFunc,
								args: []*expr{
									{
										str:     "func4",
										etype:   etFunc,
										args:    []*expr{{str: "metricA"}, {etype: etString, str: "foo"}},
										argsStr: "metricA,'foo'",
									},
								},
								argsStr: "func4(metricA,'foo')",
							},
						},
						argsStr: "",
					},
				},
				argsStr: "func3(func4(metricA,'foo')) | func2()",
			},
			nil,
			"",
		},
		{
			"metric1 | func1(func4(metricA,'foo') | func3() | func2())",
			&expr{
				str:   "func1",
				etype: etFunc,
				args: []*expr{
					{str: "metric1"},
					{str: "func2",
						etype: etFunc,
						args: []*expr{
							{str: "func3",
								etype: etFunc,
								args: []*expr{
									{str: "func4",
										etype:   etFunc,
										args:    []*expr{{str: "metricA"}, {etype: etString, str: "foo"}},
										argsStr: "metricA,'foo'",
									},
								},
								argsStr: "",
							},
						},
						argsStr: "",
					},
				},
				argsStr: "func4(metricA,'foo') | func3() | func2()",
			},
			nil,
			"",
		},
		{
			"metric1 | func1(metricA | func4('foo') | func3() | func2())",
			&expr{
				str:   "func1",
				etype: etFunc,
				args: []*expr{
					{str: "metric1"},
					{str: "func2",
						etype: etFunc,
						args: []*expr{
							{str: "func3",
								etype: etFunc,
								args: []*expr{
									{str: "func4",
										etype:   etFunc,
										args:    []*expr{{str: "metricA"}, {etype: etString, str: "foo"}},
										argsStr: "'foo'",
									},
								},
								argsStr: "",
							},
						},
						argsStr: "",
					},
				},
				argsStr: "metricA | func4('foo') | func3() | func2()",
			},
			nil,
			"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.s, func(t *testing.T) {
			e, leftover, err := Parse(tt.s, ParseContext{Piped: false, IsFullArg: false})
			if err != tt.err {
				t.Errorf("case %+v expected err %v, got %v (leftover: %q)", tt.s, tt.err, err, leftover)
				t.FailNow()
			}
			if (tt.err == nil) && (leftover != tt.leftover) {
				t.Errorf("case %+v expected leftover %q, got %q", tt.s, tt.leftover, leftover)
				t.FailNow()
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

### diff ###
%s

### leftover ###
%q`
				t.Errorf(format, tt.s, exp, got, dmp.DiffPrettyText(diffs), leftover)
				t.FailNow()
			}
		})

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
		{
			"sumSeries(seriesByTag('name=singleQuoted'))",
			"",
		},
		{
			`sumSeries(seriesByTag("name=doubleQuoted"))`,
			"",
		},
		{
			`sumSeries(seriesByTag('name=embeddedQuote"'))`,
			"",
		},
		{
			`sumSeries(seriesByTag('name=nonterminatedQuote"))`,
			"",
		},
		{
			`sumSeries(seriesByTag('name=\'escapedQuotes\''))`,
			"",
		},
		{
			"divideSeries(foo.bar;host=1;dc=test,baz.quux;host=1;dc=test)",
			"foo.bar;host=1;dc=test",
		},
	}

	for _, tt := range tests {
		if m := extractMetric(tt.in); m != tt.out {
			t.Errorf("extractMetric(%q)=%q, want %q", tt.in, m, tt.out)
		}
	}
}
