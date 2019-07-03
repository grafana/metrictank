package api

import (
	"reflect"
	"testing"
)

func TestExpressionParsing(t *testing.T) {
	type testCase struct {
		inputValue        string
		expectError       bool
		expectExpressions []string
	}
	testCases := []testCase{
		{
			inputValue:        "',=+'  , 'a=b','c=d', '~=~!', \"'[])(&%$#@!={}'\"",
			expectError:       false,
			expectExpressions: []string{",=+", "a=b", "c=d", "~=~^(?:!)", "'[])(&%$#@!={}'"},
		},
		{
			inputValue:        "'a=b',\"c=d\",'e=f'",
			expectError:       false,
			expectExpressions: []string{"a=b", "c=d", "e=f"},
		},
		{
			inputValue:        "'a=b','c=d',",
			expectError:       false,
			expectExpressions: []string{"a=b", "c=d"},
		},
		{
			inputValue:        "'a!=b','c!=d',",
			expectError:       true,
			expectExpressions: nil,
		},
		{
			inputValue:        "'a!=b','c='",
			expectError:       true,
			expectExpressions: nil,
		},
		{
			inputValue:        "'a=~[a-z'",
			expectError:       true,
			expectExpressions: nil,
		},
		{
			inputValue:        "'a=~'",
			expectError:       true,
			expectExpressions: nil,
		},
		{
			inputValue:        "'a=~.*'",
			expectError:       true,
			expectExpressions: nil,
		},
		{
			inputValue:        "'a!=~.*'",
			expectError:       false,
			expectExpressions: []string{"a!=~^(?:.*)"},
		},
		{
			inputValue:        "'a=~.+'",
			expectError:       false,
			expectExpressions: []string{"a=~^(?:.+)"},
		},
		{
			inputValue:        "",
			expectError:       true,
			expectExpressions: nil,
		},
		{
			inputValue:        "'a=b''c=d'",
			expectError:       true,
			expectExpressions: nil,
		},
		{
			inputValue:        "'a=b",
			expectError:       true,
			expectExpressions: nil,
		},
		{
			inputValue:        "''",
			expectError:       true,
			expectExpressions: nil,
		},
		{
			inputValue:        "'a=b',,'c=d'",
			expectError:       true,
			expectExpressions: nil,
		},
		{
			inputValue:        "'a=b\"",
			expectError:       true,
			expectExpressions: nil,
		},
		{
			inputValue:        "a^b=c",
			expectError:       true,
			expectExpressions: nil,
		},
		{
			inputValue:        "d!e=f",
			expectError:       true,
			expectExpressions: nil,
		},
		{
			inputValue:        "a=b;c",
			expectError:       true,
			expectExpressions: nil,
		},
		{
			inputValue:        "a=b~c",
			expectError:       true,
			expectExpressions: nil,
		},
	}

	for i, tc := range testCases {
		expressions, err := getTagQueryExpressions(tc.inputValue)

		if (err != nil) != tc.expectError {
			t.Fatalf("Got unexpected error value %q in TC:\n%+v", err, tc)
		}

		// if we expected an error we don't need to check the returned values
		if tc.expectError {
			continue
		}

		expressionsStr := expressions.Strings()
		if len(expressionsStr) != len(tc.expectExpressions) || !reflect.DeepEqual(expressionsStr, tc.expectExpressions) {
			t.Fatalf("Got unexpected expressions in TC %d\nExpected:\n%+v\nGot:\n%+v\n", i, tc.expectExpressions, expressionsStr)
		}
	}
}
