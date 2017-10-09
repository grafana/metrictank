package memory

import (
	"fmt"
	"reflect"
	"strings"
	"testing"

	"github.com/grafana/metrictank/idx"
	"gopkg.in/raintank/schema.v1"
)

var ids []idx.MetricID

func getTestIDs(t *testing.T) []idx.MetricID {
	if len(ids) > 0 {
		return ids
	}

	idStrings := []string{
		"1.02345678901234567890123456789012",
		"1.12345678901234567890123456789012",
		"1.22345678901234567890123456789012",
		"1.32345678901234567890123456789012",
		"1.42345678901234567890123456789012",
		"1.52345678901234567890123456789012",
		"1.62345678901234567890123456789012",
	}
	for _, idStr := range idStrings {
		id, err := idx.NewMetricIDFromString(idStr)
		if err != nil {
			t.Fatalf("Did not expect an error when converting id string to object: %s", idStr)
		}
		ids = append(ids, id)
	}

	return ids
}

func getTestIndex(t *testing.T) (TagIndex, map[string]*idx.Archive) {
	type testCase struct {
		id         idx.MetricID
		lastUpdate int64
		tags       []string
	}

	ids := getTestIDs(t)

	data := []testCase{
		{ids[0], 1, []string{"key1=value1", "key2=value2"}},
		{ids[1], 2, []string{"key1=value1", "key3=value3"}},
		{ids[2], 3, []string{"key1=value1", "key4=value4"}},
		{ids[3], 4, []string{"key1=value1", "key4=value3", "key3=value3"}},
		{ids[4], 5, []string{"key2=value1", "key5=value4", "key3=value3"}},
		{ids[5], 6, []string{"key2=value2", "key4=value5"}},
		{ids[6], 7, []string{"key3=value1", "key4=value4"}},
	}

	tagIdx := make(TagIndex)
	byId := make(map[string]*idx.Archive)

	for _, d := range data {
		idStr := d.id.ToString()
		byId[idStr] = &idx.Archive{}
		byId[idStr].Tags = d.tags
		byId[idStr].LastUpdate = d.lastUpdate
		for _, tag := range d.tags {
			tagSplits := strings.Split(tag, "=")
			if _, ok := tagIdx[tagSplits[0]]; !ok {
				tagIdx[tagSplits[0]] = make(TagValue)
			}

			if _, ok := tagIdx[tagSplits[0]][tagSplits[1]]; !ok {
				tagIdx[tagSplits[0]][tagSplits[1]] = make(TagIDs)
			}

			tagIdx[tagSplits[0]][tagSplits[1]][d.id] = struct{}{}
		}
	}

	return tagIdx, byId
}

func queryAndCompareResults(t *testing.T, q TagQuery, expectedData TagIDs) {
	t.Helper()
	tagIdx, byId := getTestIndex(t)

	res, err := q.Run(tagIdx, byId)
	if err != nil {
		t.Fatalf("Unexpected error when running query: %q", err)
	}

	if !reflect.DeepEqual(expectedData, res) {
		t.Fatalf("Returned data does not match expected data:\nExpected: %+v\nGot: %+v", expectedData, res)
	}
}

func TestQueryByTagSimpleEqual(t *testing.T) {
	ids := getTestIDs(t)
	q, _ := NewTagQuery([]string{"key1=value1", "key3=value3"}, 0)
	expect := make(TagIDs)
	expect[ids[1]] = struct{}{}
	expect[ids[3]] = struct{}{}
	/*expect["id2"] = struct{}{}
	expect["id4"] = struct{}{}*/
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagSimplePattern(t *testing.T) {
	ids := getTestIDs(t)
	q, _ := NewTagQuery([]string{"key4=~value[43]", "key3=~value[1-3]"}, 0)
	expect := make(TagIDs)
	/*expect["id7"] = struct{}{}
	expect["id4"] = struct{}{}*/
	expect[ids[6]] = struct{}{}
	expect[ids[3]] = struct{}{}
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagSimpleUnequal(t *testing.T) {
	ids := getTestIDs(t)
	q, _ := NewTagQuery([]string{"key1=value1", "key4!=value4"}, 0)
	expect := make(TagIDs)
	/*expect["id1"] = struct{}{}
	expect["id2"] = struct{}{}
	expect["id4"] = struct{}{}*/
	expect[ids[0]] = struct{}{}
	expect[ids[1]] = struct{}{}
	expect[ids[3]] = struct{}{}
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagSimpleNotPattern(t *testing.T) {
	ids := getTestIDs(t)
	q, _ := NewTagQuery([]string{"key1=~value?", "key4!=~value[0-9]", "key2!=~va.+"}, 0)
	expect := make(TagIDs)
	//expect["id2"] = struct{}{}
	expect[ids[1]] = struct{}{}
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagWithEqualEmpty(t *testing.T) {
	ids := getTestIDs(t)
	q, _ := NewTagQuery([]string{"key1=value1", "key2=", "key2=~"}, 0)
	expect := make(TagIDs)
	/*expect["id2"] = struct{}{}
	expect["id3"] = struct{}{}
	expect["id4"] = struct{}{}*/
	expect[ids[1]] = struct{}{}
	expect[ids[2]] = struct{}{}
	expect[ids[3]] = struct{}{}
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagWithUnequalEmpty(t *testing.T) {
	ids := getTestIDs(t)
	q, _ := NewTagQuery([]string{"key1=value1", "key3!=", "key3!=~"}, 0)
	expect := make(TagIDs)
	/*expect["id2"] = struct{}{}
	expect["id4"] = struct{}{}*/
	expect[ids[1]] = struct{}{}
	expect[ids[3]] = struct{}{}
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagInvalidQuery(t *testing.T) {
	q, _ := NewTagQuery([]string{"key!=value1"}, 0)
	tagIdx, byId := getTestIndex(t)
	_, err := q.Run(tagIdx, byId)
	if err != errInvalidQuery {
		t.Fatalf("Expected an error, but didn't get it")
	}
}

func TestTagExpressionQueryByTagWithFrom(t *testing.T) {
	tagIdx, byId := getTestIndex(t)

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
	tagIdx, byId := getTestIndex(t)
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

	idString := "1.000000000000000000000000000000%02x"
	mds := make([]schema.MetricData, 20)
	ids := make([]idx.MetricID, 20)
	for i := range mds {
		ids[i], _ = idx.NewMetricIDFromString(fmt.Sprintf(idString, i))
		mds[i].Metric = fmt.Sprintf("metric.%d", i)
		mds[i].Name = mds[i].Metric
		mds[i].Id = ids[i].ToString()
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
		expectation []idx.MetricID
	}

	testCases := []testCase{
		{
			expressions: []string{"key1=value1"},
			expectation: []idx.MetricID{ids[1], ids[11], ids[3]},
		}, {
			expressions: []string{"key1=value2"},
			expectation: []idx.MetricID{ids[18]},
		}, {
			expressions: []string{"key1=~value[0-9]"},
			expectation: []idx.MetricID{ids[1], ids[11], ids[18], ids[3]},
		}, {
			expressions: []string{"key1=~value[23]"},
			expectation: []idx.MetricID{ids[18]},
		}, {
			expressions: []string{"key1=value1", "key2=value1"},
			expectation: []idx.MetricID{},
		}, {
			expressions: []string{"key1=value1", "key2=value2"},
			expectation: []idx.MetricID{ids[1]},
		}, {
			expressions: []string{"key1=~value[12]", "key2=value2"},
			expectation: []idx.MetricID{ids[1], ids[18]},
		}, {
			expressions: []string{"key1=~value1", "key1=value2"},
			expectation: []idx.MetricID{},
		}, {
			expressions: []string{"key1=~value[0-9]", "key2=~", "key3!=value3"},
			expectation: []idx.MetricID{ids[11]},
		}, {
			expressions: []string{"key2=", "key1=value1"},
			expectation: []idx.MetricID{ids[11], ids[3]},
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
		expectationMap := make(TagIDs)
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
			key:        "key",
			value:      "=value",
			operator:   EQUAL,
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
