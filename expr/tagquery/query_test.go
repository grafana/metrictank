package tagquery

import (
	"reflect"
	"testing"
)

func TestQueryByTagFilterByTagPrefixWithEmptyString(t *testing.T) {
	_, err := NewQueryFromStrings([]string{"__tag^="}, 0)
	if err == nil {
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
				Expressions: Expressions{
					&expressionMatchTag{
						expressionCommonRe{
							expressionCommon: expressionCommon{
								key:   "__tag",
								value: "^(?:k)",
							},
							valueRe: nil,
						},
					},
					&expressionEqual{
						expressionCommon{
							key:   "a",
							value: "b",
						},
					},
					&expressionNotEqual{
						expressionCommon{
							key:   "c",
							value: "d",
						},
					},
					&expressionMatch{
						expressionCommonRe{
							expressionCommon: expressionCommon{
								key:   "e",
								value: "^(?:f)",
							},
							valueRe: nil,
						},
					},
					&expressionNotMatch{
						expressionCommonRe{
							expressionCommon: expressionCommon{
								key:   "g",
								value: "^(?:h)",
							},
							valueRe: nil,
						},
					},
					&expressionPrefix{
						expressionCommon{
							key:   "i",
							value: "j",
						},
					},
					&expressionEqual{
						expressionCommon{
							key:   "x",
							value: "z",
						},
					},
				},
				tagClause: 0,
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
			name: "missing an expression that requires non empty value because pattern matches empty value",
			args: args{
				expressionStrs: []string{"key=", "abc=~.*"},
			},
			wantErr: true,
		}, {
			name: "no error with + instead of * because pattern does not match empty value",
			args: args{
				expressionStrs: []string{"abc=~.+"},
			},
			want: Query{
				From: 0,
				Expressions: Expressions{
					&expressionMatch{
						expressionCommonRe{
							expressionCommon: expressionCommon{
								key:   "abc",
								value: "^(?:.+)",
							},
							valueRe: nil,
						},
					},
				},
				tagClause: -1,
			},
		}, {
			name: "missing an expression that requires non empty value because prefix matches empty value",
			args: args{
				expressionStrs: []string{"key=", "__tag^="},
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
				Expressions: Expressions{
					&expressionMatch{
						expressionCommonRe{
							expressionCommon: expressionCommon{
								key:   "abc",
								value: "^(?:cba)",
							},
							valueRe: nil,
						},
					},
				},
				tagClause: -1,
			},
		}, {
			name: "deduplicate duplicate expressions",
			args: args{
				expressionStrs: []string{"a=a", "b=b", "a=a"},
			},
			want: Query{
				Expressions: Expressions{
					&expressionEqual{
						expressionCommon{
							key:   "a",
							value: "a",
						},
					},
					&expressionEqual{
						expressionCommon{
							key:   "b",
							value: "b",
						},
					},
				},
				tagClause: -1,
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
			for i := range got.Expressions {
				switch got.Expressions[i].(type) {
				case *expressionMatch:
					got.Expressions[i].(*expressionMatch).valueRe = nil
				case *expressionNotMatch:
					got.Expressions[i].(*expressionNotMatch).valueRe = nil
				case *expressionMatchTag:
					got.Expressions[i].(*expressionMatchTag).valueRe = nil
				}
			}

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewQueryFromStrings():\n%v\nwant:\n%v\n", got, tt.want)
			}
		})
	}
}
