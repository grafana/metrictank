package memory

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/grafana/metrictank/idx"
	"gopkg.in/raintank/schema.v1"
)

func getTestIndex() (TagIndex, map[string]*idx.Archive) {
	type testCase struct {
		id         string
		lastUpdate int64
		tags       []string
	}
	data := []testCase{
		{"id1", 1, []string{"key1=value1", "key2=value2"}},
		{"id2", 2, []string{"key1=value1", "key3=value3"}},
		{"id3", 3, []string{"key1=value1", "key4=value4"}},
		{"id4", 4, []string{"key1=value1", "key4=value3", "key3=value3"}},
		{"id5", 5, []string{"key2=value1", "key5=value4", "key3=value3"}},
		{"id6", 6, []string{"key2=value2", "key4=value5"}},
		{"id7", 7, []string{"key3=value1", "key4=value4"}},
	}

	tagIdx := make(TagIndex)
	byId := make(map[string]*idx.Archive)

	for _, d := range data {
		byId[d.id] = &idx.Archive{}
		byId[d.id].Tags = d.tags
		byId[d.id].LastUpdate = d.lastUpdate
		for _, tag := range d.tags {
			tagSplits := strings.Split(tag, "=")
			if _, ok := tagIdx[tagSplits[0]]; !ok {
				tagIdx[tagSplits[0]] = make(map[string]map[string]struct{})
			}

			if _, ok := tagIdx[tagSplits[0]][tagSplits[1]]; !ok {
				tagIdx[tagSplits[0]][tagSplits[1]] = make(map[string]struct{})
			}

			tagIdx[tagSplits[0]][tagSplits[1]][d.id] = struct{}{}
		}
	}

	return tagIdx, byId
}

func queryAndCompareResults(t *testing.T, q TagQuery, expectedData map[string]struct{}) {
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
	q, _ := NewTagQuery([]string{"key1=value1", "key3=value3"}, 0)
	expect := make(map[string]struct{})
	expect["id2"] = struct{}{}
	expect["id4"] = struct{}{}
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagSimplePattern(t *testing.T) {
	q, _ := NewTagQuery([]string{"key4=~value[43]", "key3=~value[1-3]"}, 0)
	expect := make(map[string]struct{})
	expect["id7"] = struct{}{}
	expect["id4"] = struct{}{}
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagSimpleUnequal(t *testing.T) {
	q, _ := NewTagQuery([]string{"key1=value1", "key4!=value4"}, 0)
	expect := make(map[string]struct{})
	expect["id1"] = struct{}{}
	expect["id2"] = struct{}{}
	expect["id4"] = struct{}{}
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagSimpleNotPattern(t *testing.T) {
	q, _ := NewTagQuery([]string{"key1=~value?", "key4!=~value[0-9]", "key2!=~va.+"}, 0)
	expect := make(map[string]struct{})
	expect["id2"] = struct{}{}
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagWithEqualEmpty(t *testing.T) {
	q, _ := NewTagQuery([]string{"key1=value1", "key2=", "key2=~"}, 0)
	expect := make(map[string]struct{})
	expect["id2"] = struct{}{}
	expect["id3"] = struct{}{}
	expect["id4"] = struct{}{}
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagWithUnequalEmpty(t *testing.T) {
	q, _ := NewTagQuery([]string{"key1=value1", "key3!=", "key3!=~"}, 0)
	expect := make(map[string]struct{})
	expect["id2"] = struct{}{}
	expect["id4"] = struct{}{}
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagInvalidQuery(t *testing.T) {
	q, _ := NewTagQuery([]string{"key!=value1"}, 0)
	tagIdx, byId := getTestIndex()
	_, err := q.Run(tagIdx, byId)
	if err != errInvalidQuery {
		t.Fatalf("Expected an error, but didn't get it")
	}
}

func TestTagExpressionQueryByTagWithFrom(t *testing.T) {
	tagIdx, byId := getTestIndex()

	q, _ := NewTagQuery([]string{"key1=value1"}, 4)
	res, _ := q.Run(tagIdx, byId)
	if len(res) != 1 {
		t.Fatalf("Expected %d results, but got %d", 1, len(res))
	}

	q, _ = NewTagQuery([]string{"key1=value1"}, 3)
	res, _ = q.Run(tagIdx, byId)
	if len(res) != 2 {
		t.Fatalf("Expected %d results, but got %d", 2, len(res))
	}

	q, _ = NewTagQuery([]string{"key1=value1"}, 2)
	res, _ = q.Run(tagIdx, byId)
	if len(res) != 3 {
		t.Fatalf("Expected %d results, but got %d", 3, len(res))
	}

	q, _ = NewTagQuery([]string{"key1=value1"}, 1)
	res, _ = q.Run(tagIdx, byId)
	if len(res) != 4 {
		t.Fatalf("Expected %d results, but got %d", 4, len(res))
	}
}

func TestSingleTagQueryByTagWithFrom(t *testing.T) {
	tagIdx, byId := getTestIndex()
	memIdx := New()
	memIdx.Tags[1] = tagIdx
	memIdx.DefById = byId

	res := memIdx.Tag(1, "key1", 0)
	if res["value1"] != 4 {
		t.Fatalf("Expected %d results, but got %d", 4, len(res))
	}

	res = memIdx.Tag(1, "key1", 2)
	if res["value1"] != 3 {
		t.Fatalf("Expected %d results, but got %d", 4, len(res))
	}

	res = memIdx.Tag(1, "key1", 3)
	if res["value1"] != 2 {
		t.Fatalf("Expected %d results, but got %d", 4, len(res))
	}

	res = memIdx.Tag(1, "key1", 4)
	if res["value1"] != 1 {
		t.Fatalf("Expected %d results, but got %d", 4, len(res))
	}
}

func TestGetByTag(t *testing.T) {
	_tagSupport := tagSupport
	defer func() { tagSupport = _tagSupport }()
	tagSupport = true

	ix := New()
	ix.Init()

	mds := make([]schema.MetricData, 20)
	for i := range mds {
		mds[i].Metric = fmt.Sprintf("metric.%d", i)
		mds[i].Name = mds[i].Metric
		mds[i].Id = mds[i].Metric
		mds[i].OrgId = 1
		mds[i].Interval = 1
		mds[i].Time = 12345
	}
	mds[1].Tags = []string{"key1=value1", "key2=value2"}
	mds[11].Tags = []string{"key1=value1"}
	mds[18].Tags = []string{"key1=value2", "key2=value2"}
	mds[3].Tags = []string{"key1=value1", "key3=value3"}

	for _, md := range mds {
		ix.AddOrUpdate(&md, 1)
	}

	type testCase struct {
		expressions []string
		expectation []string
	}

	testCases := []testCase{
		{
			expressions: []string{"key1=value1"},
			expectation: []string{"metric.1", "metric.11", "metric.3"},
		}, {
			expressions: []string{"key1=value2"},
			expectation: []string{"metric.18"},
		}, {
			expressions: []string{"key1=~value[0-9]"},
			expectation: []string{"metric.1", "metric.11", "metric.18", "metric.3"},
		}, {
			expressions: []string{"key1=~value[23]"},
			expectation: []string{"metric.18"},
		}, {
			expressions: []string{"key1=value1", "key2=value1"},
			expectation: []string{},
		}, {
			expressions: []string{"key1=value1", "key2=value2"},
			expectation: []string{"metric.1"},
		}, {
			expressions: []string{"key1=~value[12]", "key2=value2"},
			expectation: []string{"metric.1", "metric.18"},
		}, {
			expressions: []string{"key1=~value1", "key1=value2"},
			expectation: []string{},
		}, {
			expressions: []string{"key1=~value[0-9]", "key2=~", "key3!=value3"},
			expectation: []string{"metric.11"},
		}, {
			expressions: []string{"key2=", "key1=value1"},
			expectation: []string{"metric.11", "metric.3"},
		},
	}

	for _, tc := range testCases {
		tagQuery, err := NewTagQuery(tc.expressions, 0)
		if err != nil {
			t.Fatalf("Got an unexpected error with query %s: %s", tc.expressions, err)
		}
		res := ix.IdsByTagQuery(1, tagQuery)
		if len(res) != len(tc.expectation) {
			t.Fatalf("Result does not match expectation for expressions %+v\nGot:\n%+v\nExpected:\n%+v\n", tc.expressions, res, tc.expectation)
		}
		expectationMap := make(map[string]struct{})
		for _, v := range tc.expectation {
			expectationMap[v] = struct{}{}
		}
		if !reflect.DeepEqual(res, expectationMap) {
			t.Fatalf("Result does not match expectation\nGot:\n%+v\nExpected:\n%+v\n", res, tc.expectation)
		}
	}
}

func TestDeleteTaggedSeries(t *testing.T) {
	_tagSupport := tagSupport
	defer func() { tagSupport = _tagSupport }()
	tagSupport = true

	ix := New()
	ix.Init()

	orgId := 1

	mds := getMetricData(orgId, 2, 50, 10, "metric.public")
	mds[10].Tags = []string{"key1=value1", "key2=value2"}

	for _, md := range mds {
		ix.AddOrUpdate(md, 1)
	}

	tagQuery, _ := NewTagQuery([]string{"key1=value1", "key2=value2"}, 0)
	res := ix.IdsByTagQuery(orgId, tagQuery)

	if len(res) != 1 {
		t.Fatalf("Expected to get 1 result, but got %d", len(res))
	}

	if len(ix.Tags[orgId]) != 2 {
		t.Fatalf("Expected tag index to contain 2 keys, but it does not: %+v", ix.Tags)
	}

	deleted, err := ix.Delete(orgId, mds[10].Metric)
	if err != nil {
		t.Fatalf("Error deleting metric: %s", err)
	}

	if len(deleted) != 1 {
		t.Fatalf("Expected 1 metric to get deleted, but got %d", len(deleted))
	}

	res = ix.IdsByTagQuery(orgId, tagQuery)

	if len(res) != 0 {
		t.Fatalf("Expected to get 0 results, but got %d", len(res))
	}

	if len(ix.Tags[orgId]) > 0 {
		t.Fatalf("Expected tag index to be empty, but it is not: %+v", ix.Tags)
	}
}

func TestExpressionParsing(t *testing.T) {
	type testCase struct {
		expression string
		key        string
		value      string
		operator   int
	}

	testCases := []testCase{
		{
			expression: "key=value",
			key:        "key",
			value:      "value",
			operator:   EQUAL,
		}, {
			expression: "key!=",
			key:        "key",
			value:      "",
			operator:   NOT_EQUAL,
		}, {
			expression: "key=",
			key:        "key",
			value:      "",
			operator:   EQUAL,
		}, {
			expression: "key=~",
			key:        "key",
			value:      "",
			operator:   MATCH,
		}, {
			expression: "key=~v_alue",
			key:        "key",
			value:      "v_alue",
			operator:   MATCH,
		}, {
			expression: "k!=~v",
			key:        "k",
			value:      "v",
			operator:   NOT_MATCH,
		}, {
			expression: "key!!=value",
			key:        "",
			value:      "",
			operator:   PARSING_ERROR,
		}, {
			expression: "key==value",
			key:        "",
			value:      "",
			operator:   PARSING_ERROR,
		}, {
			expression: "key=~=value",
			key:        "key",
			value:      "=value",
			operator:   MATCH,
		},
	}

	for _, tc := range testCases {
		expression := parseExpression(tc.expression)
		if expression.key != tc.key || expression.value != tc.value || expression.operator != tc.operator {
			t.Fatalf("Expected the values %s, %s, %d, but got %s, %s, %d", tc.key, tc.value, tc.operator, expression.key, expression.value, expression.operator)
		}
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
			NewTagQuery(expressions[i], 0)
		}
	}
}
