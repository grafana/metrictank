package tagquery

import (
	"fmt"
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
			expression: "a=value",
			key:        "a",
			value:      "value",
			operator:   EQUAL,
		}, {
			expression: "abc=",
			key:        "abc",
			value:      "",
			operator:   NOT_HAS_TAG,
		}, {
			expression: "__tag=",
			err:        true,
		}, {
			expression: "ccc!=value",
			key:        "ccc",
			value:      "value",
			operator:   NOT_EQUAL,
		}, {
			expression: "__tag=abc",
			key:        "abc",
			value:      "",
			operator:   HAS_TAG,
		}, {
			expression: "a!=",
			key:        "a",
			value:      "",
			operator:   HAS_TAG,
		}, {
			expression: "__tag!=",
			err:        true,
		}, {
			expression: "tag1=~^abc.*",
			key:        "tag1",
			value:      "^abc.*",
			operator:   MATCH,
		}, {
			expression: "abc=~",
			key:        "abc",
			value:      "",
			operator:   MATCH_ALL,
		}, {
			expression: "abc=~.*",
			key:        "abc",
			value:      "^(?:.*)",
			operator:   MATCH_ALL,
		}, {
			expression: "abc=~.+",
			key:        "abc",
			value:      "^(?:.+)",
			operator:   MATCH,
		}, {
			expression: "tag123=~.*value.*",
			key:        "tag123",
			value:      "^(?:.*value.*)",
			operator:   MATCH,
		}, {
			expression: "__tag=~.*value.*",
			key:        "__tag",
			value:      "^(?:.*value.*)",
			operator:   MATCH_TAG,
		}, {
			expression: "__tag=~",
			key:        "__tag",
			value:      "",
			operator:   MATCH_ALL,
		}, {
			expression: "__tag=~.*",
			key:        "__tag",
			value:      "^(?:.*)",
			operator:   MATCH_ALL,
		}, {
			expression: "__tag=~.+",
			key:        "__tag",
			value:      "^(?:.+)",
			operator:   MATCH_TAG,
		}, {
			expression: "abc!=~.*",
			key:        "abc",
			value:      "^(?:.*)",
			operator:   MATCH_NONE,
		}, {
			expression: "key!=~v_alue",
			key:        "key",
			value:      "^(?:v_alue)",
			operator:   NOT_MATCH,
		}, {
			expression: "k!=~",
			key:        "k",
			value:      "",
			operator:   MATCH_NONE,
		}, {
			expression: "k!=~.*",
			key:        "k",
			value:      "^(?:.*)",
			operator:   MATCH_NONE,
		}, {
			expression: "sometag!=~.*abc.*",
			key:        "sometag",
			value:      "^(?:.*abc.*)",
			operator:   NOT_MATCH,
		}, {
			expression: "tag1^=",
			key:        "tag1",
			value:      "",
			operator:   MATCH_ALL,
		}, {
			expression: "tag1^=abc",
			key:        "tag1",
			value:      "abc",
			operator:   PREFIX,
		}, {
			expression: "__tag^=a",
			key:        "__tag",
			value:      "a",
			operator:   PREFIX_TAG,
		}, {
			expression: "__tag^=",
			key:        "__tag",
			value:      "",
			operator:   MATCH_ALL,
		}, {
			expression: "key!!=value",
			err:        true,
		}, {
			expression: "key==value",
			key:        "key",
			value:      "=value",
			operator:   EQUAL,
		}, {
			expression: "key=~=value",
			key:        "key",
			value:      "^(?:=value)",
			operator:   MATCH,
		}, {
			expression: "__tag=~key",
			key:        "__tag",
			value:      "^(?:key)",
			operator:   MATCH_TAG,
		}, {
			expression: "__tag^=some.key",
			key:        "__tag",
			value:      "some.key",
			operator:   PREFIX_TAG,
		}, {
			expression: "key=~(abc",
			err:        true,
		}, {
			expression: "=key=abc",
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
			if (err != nil) != tc.err {
				if tc.err {
					t.Fatalf("Expected error, but did not get one")
				} else {
					t.Fatalf("Did not expect error, but got one: %q", err)
				}
			}
			if err == nil && (expression.GetKey() != tc.key || expression.GetValue() != tc.value || expression.GetOperator() != tc.operator) {
				t.Fatalf("Expected the values %s, %s, %d, %t, but got %s, %s, %d, %q", tc.key, tc.value, tc.operator, tc.err, expression.GetKey(), expression.GetValue(), expression.GetOperator(), err)
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

			e.StringIntoWriter(&builder)
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
			if !e1.Equals(e2) {
				t.Fatalf("Expected two instantiations of expressions to be equal, but they were not: \"%s\"", tc.expression)
			}

			for j := range tc.notEqual {
				other, err := ParseExpression(tc.notEqual[j])
				if err != nil {
					t.Fatalf("Unexpected parsing error of \"%s\": %s", tc.notEqual[j], err)
				}

				if e1.Equals(other) || e2.Equals(other) {
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
