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
		Queries: Expressions{
			{
				Tag: Tag{
					Key:   "e",
					Value: "f",
				},
				Operator: NOT_EQUAL,
			}, {
				Tag: Tag{
					Key:   "g",
					Value: "h",
				},
				Operator: PREFIX,
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
	_, err := ParseMetaTagRecord([]string{"a=b"}, []string{"c=~~d"})
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
