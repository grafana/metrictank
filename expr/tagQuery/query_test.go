package tagQuery

import (
	"reflect"
	"testing"
)

func TestQueryByTagFilterByTagPrefixWithEmptyString(t *testing.T) {
	_, err := NewQueryFromStrings([]string{"__tag^="}, 0)
	if err != errInvalidQuery {
		t.Fatalf("Expected an error, but didn't get it")
	}
}

func TestQueryByTagInvalidQuery(t *testing.T) {
	_, err := NewQueryFromStrings([]string{"key!=value1"}, 0)
	if err != errInvalidQuery {
		t.Fatalf("Expected an error, but didn't get it")
	}
}

func TestNewQueryFromStrings(t *testing.T) {
	type args struct {
		expressionStrs []string
		from           int64
	}
	tests := []struct {
		name    string
		args    args
		want    Query
		wantErr bool
	}{
		{
			name: "various simple expressions",
			args: args{
				expressionStrs: []string{"a=b", "x=z", "c!=d", "e=~f", "g!=~h", "i^=j", "__tag=~k"},
				from:           321,
			},
			want: Query{
				From: 321,
				Expressions: map[ExpressionOperator]Expressions{
					EQUAL: {
						{
							Tag:                   Tag{Key: "a", Value: "b"},
							Operator:              EQUAL,
							RequiresNonEmptyValue: true,
						}, {
							Tag:                   Tag{Key: "x", Value: "z"},
							Operator:              EQUAL,
							RequiresNonEmptyValue: true,
						},
					},
					NOT_EQUAL: {
						{
							Tag:      Tag{Key: "c", Value: "d"},
							Operator: NOT_EQUAL,
						},
					},
					MATCH: {
						{
							Tag:                   Tag{Key: "e", Value: "^(?:f)"},
							Operator:              MATCH,
							RequiresNonEmptyValue: true,
							UsesRegex:             true,
						},
					},
					NOT_MATCH: {
						{
							Tag:       Tag{Key: "g", Value: "^(?:h)"},
							Operator:  NOT_MATCH,
							UsesRegex: true,
						},
					},
					PREFIX: {
						{
							Tag:                   Tag{Key: "i", Value: "j"},
							Operator:              PREFIX,
							RequiresNonEmptyValue: true,
						},
					},
				},
				TagMatch: Expression{
					Tag:                   Tag{Key: "__tag", Value: "^(?:k)"},
					Operator:              MATCH_TAG,
					RequiresNonEmptyValue: true,
					UsesRegex:             true,
				},
				TagClause: MATCH_TAG,
				StartWith: EQUAL,
			},
		}, {
			name: "test tag prefix with empty value",
			args: args{
				expressionStrs: []string{"__tag^="},
			},
			wantErr: true,
		}, {
			name: "missing an expression that requires non empty value",
			args: args{
				expressionStrs: []string{"key!=value"},
			},
			wantErr: true,
		}, {
			name: "missing an expression that requires non empty value because of equal with empty value",
			args: args{
				expressionStrs: []string{"key=", "abc!=cba"},
			},
			wantErr: true,
		}, {
			name: "two different tag queries",
			args: args{
				expressionStrs: []string{"__tag^=abc", "__tag=~cba"},
			},
			wantErr: true,
		}, {
			name: "start with match",
			args: args{
				expressionStrs: []string{"abc=~cba"},
			},
			want: Query{
				Expressions: map[ExpressionOperator]Expressions{
					MATCH: {
						{
							Tag:                   Tag{Key: "abc", Value: "^(?:cba)"},
							Operator:              MATCH,
							RequiresNonEmptyValue: true,
							UsesRegex:             true,
						},
					},
				},
				StartWith: MATCH,
			},
		}, {
			name: "deduplicate duplicate expressions",
			args: args{
				expressionStrs: []string{"a=a", "b=b", "a=a"},
			},
			want: Query{
				Expressions: map[ExpressionOperator]Expressions{
					EQUAL: {
						{
							Tag:                   Tag{Key: "a", Value: "a"},
							Operator:              EQUAL,
							RequiresNonEmptyValue: true,
						}, {
							Tag:                   Tag{Key: "b", Value: "b"},
							Operator:              EQUAL,
							RequiresNonEmptyValue: true,
						},
					},
				},
				StartWith: EQUAL,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := NewQueryFromStrings(tt.args.expressionStrs, tt.args.from)
			if (err != nil) != tt.wantErr {
				t.Errorf("NewQueryFromStrings() error = %v, wantErr %v", err, tt.wantErr)
				return
			}

			if tt.wantErr {
				// if we expect an error, then we don't need to look at the other returned value
				return
			}

			// don't compare the compiled regex objects
			got.TagMatch.Regex = nil
			for operator := range got.Expressions {
				for i := range got.Expressions[operator] {
					got.Expressions[operator][i].Regex = nil
				}
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewQueryFromStrings():\n%v\nwant:\n%v\n", got, tt.want)
			}
		})
	}
}
