package expr

import (
	"reflect"
	"testing"
)

// here we use smartSummarize because it has multiple optional arguments which allows us to test some interesting things
func TestNewPlan(t *testing.T) {

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
				{"foo.bar.*", from, to},
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
				{"foo.bar.*", from, to},
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
				{"foo.bar.*", from, to},
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
				{"foo.bar.*", from, to},
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
				{"foo.bar.*", from, to},
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
				{"foo.bar.*", from, to},
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
		req, err := newplanFunc(e, fn, from, to, stable, nil)
		if !reflect.DeepEqual(err, c.expErr) {
			t.Errorf("case %d: %q, expected error %v - got %v", i, c.name, c.expErr, err)
		}
		if !reflect.DeepEqual(req, c.expReq) {
			t.Errorf("case %d: %q, expected req %v - got %v", i, c.name, c.expReq, req)
		}
	}
}
