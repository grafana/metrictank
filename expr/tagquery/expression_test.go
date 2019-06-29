package tagquery

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestExpressionParsing(t *testing.T) {
	type testCase struct {
		expression string
		key        string
		value      string
		operator   ExpressionOperator
		err        bool
	}

	testCases := []testCase{
		{
			expression: "key=value",
			key:        "key",
			value:      "value",
			operator:   EQUAL,
			err:        false,
		}, {
			expression: "key!=",
			key:        "key",
			value:      "",
			operator:   HAS_TAG,
			err:        false,
		}, {
			expression: "key=",
			key:        "key",
			value:      "",
			operator:   NOT_HAS_TAG,
			err:        false,
		}, {
			expression: "key=~",
			key:        "key",
			value:      "",
			operator:   NOT_HAS_TAG,
			err:        false,
		}, {
			expression: "key=~v_alue",
			key:        "key",
			value:      "^(?:v_alue)",
			operator:   MATCH,
			err:        false,
		}, {
			expression: "k!=~v",
			key:        "k",
			value:      "^(?:v)",
			operator:   NOT_MATCH,
			err:        false,
		}, {
			expression: "key!!=value",
			err:        true,
		}, {
			expression: "key==value",
			key:        "key",
			value:      "=value",
			operator:   EQUAL,
			err:        false,
		}, {
			expression: "key=~=value",
			key:        "key",
			value:      "^(?:=value)",
			operator:   MATCH,
			err:        false,
		}, {
			expression: "__tag=~key",
			key:        "__tag",
			value:      "^(?:key)",
			operator:   MATCH_TAG,
			err:        false,
		}, {
			expression: "__tag^=some.key",
			key:        "__tag",
			value:      "some.key",
			operator:   PREFIX_TAG,
			err:        false,
		}, {
			expression: "key=~(abc",
			err:        true,
		}, {
			expression: "__tag!=some.key",
			err:        true,
		}, {
			expression: "key",
			err:        true,
		},
	}

	for i, tc := range testCases {
		t.Run(fmt.Sprintf("TC %d \"%s\"", i, tc.expression), func(t *testing.T) {
			expression, err := ParseExpression(tc.expression)
			if (err != nil) != tc.err || (err == nil && (expression.GetKey() != tc.key || expression.GetValue() != tc.value || expression.GetOperator() != tc.operator)) {
				t.Fatalf("Expected the values %s, %s, %d, %t, but got %s, %s, %d, %q", tc.key, tc.value, tc.operator, tc.err, expression.GetKey(), expression.GetValue(), expression.GetOperator(), err)
			}
		})
	}
}

func TestExpressions_Sort(t *testing.T) {
	tests := make([]struct {
		name string
		have Expressions
		want Expressions
	}, 1)

	tests[0].name = "simple sort"
	tests[0].have, _ = ParseExpressions([]string{"a!=a", "b=a", "a=b", "a=a"})
	tests[0].want, _ = ParseExpressions([]string{"a=a", "a=b", "b=a", "a!=a"})

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.have.SortByFilterOrder()
			if !reflect.DeepEqual(tt.have, tt.want) {
				t.Fatalf("Expected expressions to be sorted:\nExpected:\n%+v\nGot:\n%+v\n", tt.want, tt.have)
			}
		})
	}
}

func TestExpressionsParsingAndBackToString(t *testing.T) {
	tests := make([]struct {
		got    string
		expect string
	}, 8)

	tests[0].got = "a=b"
	tests[0].expect = "a=b"
	tests[1].got = "ccc!=$@#@"
	tests[1].expect = "ccc!=$@#@"
	tests[2].got = "~=~!"
	tests[2].expect = "~=~^(?:!)"
	tests[3].got = "d=~e"
	tests[3].expect = "d=~^(?:e)"
	tests[4].got = "f!=~g"
	tests[4].expect = "f!=~^(?:g)"
	tests[5].got = "h^=i"
	tests[5].expect = "h^=i"
	tests[6].got = "__tag^=q"
	tests[6].expect = "__tag^=q"
	tests[7].got = "__tag=~abc"
	tests[7].expect = "__tag=~^(?:abc)"

	builder := strings.Builder{}
	for i, tc := range tests {
		t.Run(fmt.Sprintf("TC %d (%s)", i, tc.got), func(t *testing.T) {
			e, err := ParseExpression(tc.got)
			if err != nil {
				t.Fatalf("Error when parsing expression: %s", err)
			}

			e.StringIntoBuilder(&builder)
			res := builder.String()
			builder.Reset()

			if res != tc.expect {
				t.Fatalf("Expected expression \"%s\", but got \"%s\"", tc.expect, res)
			}
		})
	}
}

func TestExpression_IsEqualTo(t *testing.T) {
	tests := make([]struct {
		expression string
		notEqual   []string
	}, 3)

	tests[0].expression = "a=b"
	tests[0].notEqual = []string{"a!=b", "a=a", "b=b"}
	tests[1].expression = "a!=b"
	tests[1].notEqual = []string{"a=b", "a!=a", "b!=b"}
	tests[2].expression = "a=~b"
	tests[2].notEqual = []string{"a=b", "a!=~b", "a=~b", "b=~b"}
	tests[2].expression = "__tag=a"
	tests[2].notEqual = []string{"tag=a", "__tag^=a", "__tag=~a", "__tag=b"}
	tests[2].expression = "a="
	tests[2].notEqual = []string{"a=b", "b=", "a=~b", "b=~"}

	for i, tc := range tests {
		t.Run(fmt.Sprintf("TC %d \"%s\"", i, tc.expression), func(t *testing.T) {
			e1, err := ParseExpression(tc.expression)
			if err != nil {
				t.Fatalf("Unexpected parsing error of \"%s\": %s", tc.expression, err)
			}
			e2, err := ParseExpression(tc.expression)
			if err != nil {
				t.Fatalf("Unexpected parsing error of \"%s\": %s", tc.expression, err)
			}
			if !ExpressionsAreEqual(e1, e2) {
				t.Fatalf("Expected two instantiations of expressions to be equal, but they were not: \"%s\"", tc.expression)
			}

			for j := range tc.notEqual {
				other, err := ParseExpression(tc.notEqual[j])
				if err != nil {
					t.Fatalf("Unexpected parsing error of \"%s\": %s", tc.notEqual[j], err)
				}

				if ExpressionsAreEqual(e1, other) || ExpressionsAreEqual(e2, other) {
					t.Fatalf("Expressions are supposed to not be equal, but they were: \"%s\"/\"%s\"", tc.expression, tc.notEqual[j])
				}
			}
		})
	}
}

func BenchmarkExpressionParsing(b *testing.B) {
	expressions := [][]string{
		{"key=value", "key!=value"},
		{"key=~value", "key!=~value"},
		{"key1=~", "key2=~"},
		{"key1!=~aaa", "key2=~abc"},
	}

	b.ReportAllocs()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		for i := 0; i < 4; i++ {
			NewQueryFromStrings(expressions[i], 0)
		}
	}
}
