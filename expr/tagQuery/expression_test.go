package tagQuery

import (
	"reflect"
	"regexp"
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
			operator:   NOT_EQUAL,
			err:        false,
		}, {
			expression: "key=",
			key:        "key",
			value:      "",
			operator:   EQUAL,
			err:        false,
		}, {
			expression: "key=~",
			key:        "key",
			value:      "",
			operator:   MATCH,
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

	for _, tc := range testCases {
		expression, err := ParseExpression(tc.expression)
		if (err != nil) != tc.err || (err == nil && (expression.Key != tc.key || expression.Value != tc.value || expression.Operator != tc.operator)) {
			t.Fatalf("Expected the values %s, %s, %d, %t, but got %s, %s, %d, %q", tc.key, tc.value, tc.operator, tc.err, expression.Key, expression.Value, expression.Operator, err)
		}
	}
}

func TestExpressions_Sort(t *testing.T) {
	tests := []struct {
		name string
		e    Expressions
		want Expressions
	}{
		{
			name: "simple sort",
			e: Expressions{
				{
					Tag:      Tag{Key: "a", Value: "a"},
					Operator: NOT_EQUAL,
				}, {
					Tag:      Tag{Key: "b", Value: "a"},
					Operator: EQUAL,
				}, {
					Tag:      Tag{Key: "a", Value: "b"},
					Operator: EQUAL,
				}, {
					Tag:      Tag{Key: "a", Value: "a"},
					Operator: EQUAL,
				},
			},
			want: Expressions{
				{
					Tag:      Tag{Key: "a", Value: "a"},
					Operator: EQUAL,
				}, {
					Tag:      Tag{Key: "a", Value: "a"},
					Operator: NOT_EQUAL,
				}, {
					Tag:      Tag{Key: "a", Value: "b"},
					Operator: EQUAL,
				}, {
					Tag:      Tag{Key: "b", Value: "a"},
					Operator: EQUAL,
				},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			tt.e.Sort()
			if !reflect.DeepEqual(tt.e, tt.want) {
				t.Fatalf("Expected expressions to be sorted:\nExpected:\n%+v\nGot:\n%+v\n", tt.want, tt.e)
			}
		})
	}
}

func TestExpressions_Strings(t *testing.T) {
	tests := []struct {
		name string
		e    Expressions
		want []string
	}{
		{
			name: "simple expressions",
			e: Expressions{
				{
					Tag:      Tag{Key: "a", Value: "b"},
					Operator: EQUAL,
				}, {
					Tag:      Tag{Key: "ccc", Value: "$@#@"},
					Operator: NOT_EQUAL,
				}, {
					Tag:      Tag{Key: "~", Value: "!"},
					Operator: MATCH,
				}, {
					Tag:      Tag{Key: "d", Value: "e"},
					Operator: MATCH_TAG,
				}, {
					Tag:      Tag{Key: "f", Value: "g"},
					Operator: NOT_MATCH,
				}, {
					Tag:      Tag{Key: "h", Value: "i"},
					Operator: PREFIX,
				}, {
					Tag:      Tag{Key: "j", Value: "q"},
					Operator: PREFIX_TAG,
				},
			},
			want: []string{"a=b", "ccc!=$@#@", "~=~!", "__tag=~e", "f!=~g", "h^=i", "__tag^=q"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.e.Strings(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Expressions.Strings() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestExpression_IsEqualTo(t *testing.T) {
	type fields struct {
		Tag                   Tag
		Operator              ExpressionOperator
		RequiresNonEmptyValue bool
		UsesRegex             bool
		Regex                 *regexp.Regexp
	}
	type args struct {
		other Expression
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   bool
	}{
		{
			name: "Equal expressions with different internal settings",
			fields: fields{
				Tag:                   Tag{Key: "a", Value: "b"},
				Operator:              EQUAL,
				RequiresNonEmptyValue: false,
				UsesRegex:             false,
			},
			args: args{
				other: Expression{
					Tag:                   Tag{Key: "a", Value: "b"},
					Operator:              EQUAL,
					RequiresNonEmptyValue: true,
					UsesRegex:             true,
				},
			},
			want: true,
		}, {
			name: "Different key",
			fields: fields{
				Tag:      Tag{Key: "a", Value: "b"},
				Operator: EQUAL,
			},
			args: args{
				other: Expression{
					Tag:      Tag{Key: "b", Value: "b"},
					Operator: EQUAL,
				},
			},
			want: false,
		}, {
			name: "Different value",
			fields: fields{
				Tag:      Tag{Key: "a", Value: "a"},
				Operator: EQUAL,
			},
			args: args{
				other: Expression{
					Tag:      Tag{Key: "a", Value: "b"},
					Operator: EQUAL,
				},
			},
			want: false,
		}, {
			name: "Different operator",
			fields: fields{
				Tag:      Tag{Key: "a", Value: "b"},
				Operator: EQUAL,
			},
			args: args{
				other: Expression{
					Tag:      Tag{Key: "a", Value: "b"},
					Operator: NOT_EQUAL,
				},
			},
			want: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e := &Expression{
				Tag:                   tt.fields.Tag,
				Operator:              tt.fields.Operator,
				RequiresNonEmptyValue: tt.fields.RequiresNonEmptyValue,
				UsesRegex:             tt.fields.UsesRegex,
				Regex:                 tt.fields.Regex,
			}
			if got := e.IsEqualTo(tt.args.other); got != tt.want {
				t.Errorf("Expression.IsEqualTo() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestParseExpressionAndBackToString(t *testing.T) {
	expressions := []string{
		"a=b",
		"a=b",
		"a!=b",
		"a!=b",
		"a=~b",
		"a=~^(?:b)",
		"a!=~b",
		"a!=~^(?:b)",
		"a^=b",
		"a^=b",
		"__tag=~abc",
		"__tag=~^(?:abc)",
		"__tag^=cba",
		"__tag^=cba",
	}

	builder := strings.Builder{}
	for i := 0; i < len(expressions); i += 2 {
		parsed, err := ParseExpression(expressions[i])
		if err != nil {
			t.Fatalf("TC %d: Unexpected error: %s", i, err)
		}
		parsed.StringIntoBuilder(&builder)
		toString := builder.String()
		expected := expressions[i+1]
		if toString != expected {
			t.Fatalf("TC %d: After parsing and converting back to string, expressions has changed unexpectedly: \"%s\" / \"%s\"", i, toString, expected)
		}
		builder.Reset()
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
