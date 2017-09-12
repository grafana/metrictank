package memory

import (
	"reflect"
	"strings"
	"testing"

	"github.com/raintank/metrictank/idx"
)

func getTestIndex() (TagIndex, map[string]*idx.Archive) {
	data := [][]string{
		{"id1", "key1=value1", "key2=value2"},
		{"id2", "key1=value1", "key3=value3"},
		{"id3", "key1=value1", "key4=value4"},
		{"id4", "key1=value1", "key4=value3", "key3=value3"},
		{"id5", "key2=value1", "key5=value4", "key3=value3"},
		{"id6", "key2=value2", "key4=value5"},
		{"id7", "key3=value1", "key4=value4"},
	}

	tagIdx := make(TagIndex)
	byId := make(map[string]*idx.Archive)

	for _, d := range data {
		byId[d[0]] = &idx.Archive{}
		byId[d[0]].Tags = d[1:]
		for _, tag := range d[1:] {
			tagSplits := strings.Split(tag, "=")
			if _, ok := tagIdx[tagSplits[0]]; !ok {
				tagIdx[tagSplits[0]] = make(map[string]map[string]struct{})
			}

			if _, ok := tagIdx[tagSplits[0]][tagSplits[1]]; !ok {
				tagIdx[tagSplits[0]][tagSplits[1]] = make(map[string]struct{})
			}

			tagIdx[tagSplits[0]][tagSplits[1]][d[0]] = struct{}{}
		}
	}

	return tagIdx, byId
}

func queryAndCompareResults(t *testing.T, q *TagQuery, expectedData map[string]struct{}) {
	t.Helper()
	tagIdx, byId := getTestIndex()

	res, err := q.Run(tagIdx, byId)
	if err != nil {
		t.Fatalf("Unexpected error when running query: %q", err)
	}

	if !reflect.DeepEqual(expectedData, res) {
		t.Fatalf("Returned data does not match expected data:\nExpected: %+v\nGot: %+v", expectedData, res)
	}
}

func TestQueryByTagSimpleEqual(t *testing.T) {
	q, _ := NewTagQuery([]string{"key1=value1", "key3=value3"})
	expect := make(map[string]struct{})
	expect["id2"] = struct{}{}
	expect["id4"] = struct{}{}
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagSimplePattern(t *testing.T) {
	q, _ := NewTagQuery([]string{"key4=~value[43]", "key3=~value[1-3]"})
	expect := make(map[string]struct{})
	expect["id7"] = struct{}{}
	expect["id4"] = struct{}{}
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagSimpleUnequal(t *testing.T) {
	q, _ := NewTagQuery([]string{"key1=value1", "key4!=value4"})
	expect := make(map[string]struct{})
	expect["id1"] = struct{}{}
	expect["id2"] = struct{}{}
	expect["id4"] = struct{}{}
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagSimpleNotPattern(t *testing.T) {
	q, _ := NewTagQuery([]string{"key1=~value?", "key4!=~value[0-9]", "key2!=~va.+"})
	expect := make(map[string]struct{})
	expect["id2"] = struct{}{}
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagWithEqualEmpty(t *testing.T) {
	q, _ := NewTagQuery([]string{"key1=value1", "key2=", "key2=~"})
	expect := make(map[string]struct{})
	expect["id2"] = struct{}{}
	expect["id3"] = struct{}{}
	expect["id4"] = struct{}{}
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagWithUnequalEmpty(t *testing.T) {
	q, _ := NewTagQuery([]string{"key1=value1", "key3!=", "key3!=~"})
	expect := make(map[string]struct{})
	expect["id2"] = struct{}{}
	expect["id4"] = struct{}{}
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagInvalidQuery(t *testing.T) {
	_, err := NewTagQuery([]string{"key!=value1"})
	if err != errInvalidQuery {
		t.Fatalf("Expected an error, but didn't get it")
	}
}
