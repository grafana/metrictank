package tagquery

import (
	"reflect"
	"testing"
)

func TestParseMetaTagRecord(t *testing.T) {
	record, err := ParseMetaTagRecord([]string{"a=b", "c=d"}, []string{"e!=f", "g^=h"})
	if err != nil {
		t.Fatalf("Unexpected error when parsing meta tag record: %s", err)
	}

	reflect.DeepEqual(record, MetaTagRecord{
		MetaTags: Tags{
			{
				Key:   "a",
				Value: "b",
			}, {
				Key:   "c",
				Value: "d",
			},
		},
		Expressions: Expressions{
			&expressionNotEqual{
				expressionCommon{
					key:   "e",
					value: "f",
				},
			},
			&expressionPrefix{
				expressionCommon{
					key:   "g",
					value: "h",
				},
			},
		},
	})
}

func TestErrorOnParsingMetaTagRecordWithInvalidTag(t *testing.T) {
	_, err := ParseMetaTagRecord([]string{"a^=b"}, []string{"c=d"})
	if err == nil {
		t.Fatalf("Expected an error, but did not get one")
	}
}

func TestErrorOnParsingMetaTagRecordWithInvalidQuery(t *testing.T) {
	_, err := ParseMetaTagRecord([]string{"a=b"}, []string{"c^^=~d"})
	if err == nil {
		t.Fatalf("Expected an error, but did not get one")
	}
}

func TestErrorOnParsingMetaTagRecordWithoutQueryy(t *testing.T) {
	_, err := ParseMetaTagRecord([]string{"a=b"}, []string{})
	if err == nil {
		t.Fatalf("Expected an error, but did not get one")
	}
}

func TestErrorOnParsingMetaTagRecordWithValidExpressionsButInvalidQuery(t *testing.T) {
	// the given expression is valid, but as a query this set of expressions is not valid
	// because every query must have at least one expression requiring a non-empty value
	_, err := ParseMetaTagRecord([]string{"meta=tag"}, []string{"a!=b"})
	if err == nil {
		t.Fatalf("Expected an error, but did not get one")
	}
}
