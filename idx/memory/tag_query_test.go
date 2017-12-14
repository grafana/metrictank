package memory

import (
	"fmt"
	"reflect"
	"sort"
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
		"1.72345678901234567890123456789012",
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
		{ids[3], 4, []string{"key1=value1", "abc=bbb", "key4=value3", "key3=value3"}},
		{ids[4], 5, []string{"key2=value1", "key5=value4", "key3=value3"}},
		{ids[5], 6, []string{"key2=value2", "aaa=bbb", "abc=bbb", "key4=value5", "key3=vabc"}},
		{ids[6], 7, []string{"key3=value1", "aaa=bbb", "key4=value4"}},
		{ids[7], 8, []string{"key3=valxxx"}},
	}

	tagIdx := make(TagIndex)
	byId := make(map[string]*idx.Archive)

	for i, d := range data {
		idStr := d.id.String()
		byId[idStr] = &idx.Archive{}
		byId[idStr].Name = fmt.Sprintf("metric%d", i)
		byId[idStr].Tags = d.tags
		byId[idStr].LastUpdate = d.lastUpdate
		for _, tag := range d.tags {
			tagSplits := strings.Split(tag, "=")
			tagIdx.addTagId(tagSplits[0], tagSplits[1], d.id)
		}
		tagIdx.addTagId("name", byId[idStr].Name, d.id)
	}

	return tagIdx, byId
}

func queryAndCompareTagResults(t *testing.T, q TagQuery, expectedData map[string]struct{}) {
	t.Helper()
	tagIdx, byId := getTestIndex(t)

	res := q.RunGetTags(tagIdx, byId)
	if !reflect.DeepEqual(expectedData, res) {
		t.Fatalf("Expected: %+v\nGot: %+v", expectedData, res)
	}
}

func queryAndCompareResults(t *testing.T, q TagQuery, expectedData TagIDs) {
	t.Helper()
	tagIdx, byId := getTestIndex(t)

	res := q.Run(tagIdx, byId)

	if !reflect.DeepEqual(expectedData, res) {
		toStr := func(ids TagIDs) string {
			var res string
			for id := range ids {
				if len(res) > 0 {
					res += " "
				}
				res += id.String()
			}
			return res
		}
		t.Fatalf("Returned data does not match expected data:\nExpected: %s\nGot: %s", toStr(expectedData), toStr(res))
	}
}

func TestQueryByTagSimpleEqual(t *testing.T) {
	ids := getTestIDs(t)
	q, _ := NewTagQuery([]string{"key1=value1", "key3=value3"}, 0)
	expect := make(TagIDs)
	expect[ids[1]] = struct{}{}
	expect[ids[3]] = struct{}{}
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagSimplePrefix(t *testing.T) {
	ids := getTestIDs(t)
	q, _ := NewTagQuery([]string{"key3^=val"}, 0)
	expect := make(TagIDs)
	expect[ids[1]] = struct{}{}
	expect[ids[3]] = struct{}{}
	expect[ids[4]] = struct{}{}
	expect[ids[6]] = struct{}{}
	expect[ids[7]] = struct{}{}
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagPrefixSpecialCaseName(t *testing.T) {
	ids := getTestIDs(t)
	q, _ := NewTagQuery([]string{"key1=value1", "name^=metric2"}, 0)
	expect := make(TagIDs)
	expect[ids[2]] = struct{}{}
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagFilterByPrefix(t *testing.T) {
	ids := getTestIDs(t)
	q, _ := NewTagQuery([]string{"key1=value1", "key3^=val"}, 0)
	expect := make(TagIDs)
	expect[ids[1]] = struct{}{}
	expect[ids[3]] = struct{}{}
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagSimplePattern(t *testing.T) {
	ids := getTestIDs(t)
	q, _ := NewTagQuery([]string{"key4=~value[43]", "key3=~value[1-3]"}, 0)
	expect := make(TagIDs)
	expect[ids[6]] = struct{}{}
	expect[ids[3]] = struct{}{}
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagSimpleUnequal(t *testing.T) {
	ids := getTestIDs(t)
	q, _ := NewTagQuery([]string{"key1=value1", "key4!=value4"}, 0)
	expect := make(TagIDs)
	expect[ids[0]] = struct{}{}
	expect[ids[1]] = struct{}{}
	expect[ids[3]] = struct{}{}
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagSimpleNotPattern(t *testing.T) {
	ids := getTestIDs(t)
	q, _ := NewTagQuery([]string{"key1=~value?", "key4!=~value[0-9]", "key2!=~va.+"}, 0)
	expect := make(TagIDs)
	expect[ids[1]] = struct{}{}
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagWithEqualEmpty(t *testing.T) {
	ids := getTestIDs(t)
	q, _ := NewTagQuery([]string{"key1=value1", "key2=", "key2=~"}, 0)
	expect := make(TagIDs)
	expect[ids[1]] = struct{}{}
	expect[ids[2]] = struct{}{}
	expect[ids[3]] = struct{}{}
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagWithUnequalEmpty(t *testing.T) {
	ids := getTestIDs(t)
	q, _ := NewTagQuery([]string{"key1=value1", "key3!=", "key3!=~"}, 0)
	expect := make(TagIDs)
	expect[ids[1]] = struct{}{}
	expect[ids[3]] = struct{}{}
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagNameEquals(t *testing.T) {
	ids := getTestIDs(t)
	q, _ := NewTagQuery([]string{"key1=value1", "key3=value3", "name=metric1"}, 0)
	expect := make(TagIDs)
	expect[ids[1]] = struct{}{}
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagNameRegex(t *testing.T) {
	ids := getTestIDs(t)
	q, _ := NewTagQuery([]string{"key1=value1", "key3=value3", "name=~metr"}, 0)
	expect := make(TagIDs)
	expect[ids[1]] = struct{}{}
	expect[ids[3]] = struct{}{}
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagFilterByTagMatch(t *testing.T) {
	q, _ := NewTagQuery([]string{"__tag=~a{1}"}, 0)
	expectTags := make(map[string]struct{})
	expectTags["abc"] = struct{}{}
	expectTags["aaa"] = struct{}{}
	queryAndCompareTagResults(t, q, expectTags)

	q, _ = NewTagQuery([]string{"__tag=~a{2}"}, 0)
	delete(expectTags, "abc")
	queryAndCompareTagResults(t, q, expectTags)

	q, _ = NewTagQuery([]string{"__tag=~a{3}"}, 0)
	queryAndCompareTagResults(t, q, expectTags)

	q, _ = NewTagQuery([]string{"__tag=~a{4}"}, 0)
	delete(expectTags, "aaa")
	queryAndCompareTagResults(t, q, expectTags)
}

func TestQueryByTagFilterByTagPrefix(t *testing.T) {
	q, _ := NewTagQuery([]string{"__tag^=a"}, 0)
	expectTags := make(map[string]struct{})
	expectTags["abc"] = struct{}{}
	expectTags["aaa"] = struct{}{}
	queryAndCompareTagResults(t, q, expectTags)

	q, _ = NewTagQuery([]string{"__tag^=aa"}, 0)
	delete(expectTags, "abc")
	queryAndCompareTagResults(t, q, expectTags)

	q, _ = NewTagQuery([]string{"__tag^=aaa"}, 0)
	queryAndCompareTagResults(t, q, expectTags)

	q, _ = NewTagQuery([]string{"__tag^=aaaa"}, 0)
	delete(expectTags, "aaa")
	queryAndCompareTagResults(t, q, expectTags)
}

func TestQueryByTagFilterByTagPrefixSpecialCaseName(t *testing.T) {
	q, _ := NewTagQuery([]string{"key1=value1", "__tag^=na"}, 0)
	expectTags := make(map[string]struct{})
	expectTags["name"] = struct{}{}
	queryAndCompareTagResults(t, q, expectTags)
}

func TestQueryByTagFilterByTagMatchWithExpressionAndNameException(t *testing.T) {
	ids := getTestIDs(t)
	q, _ := NewTagQuery([]string{"__tag=~na", "key2=value2"}, 0)
	if q.startWith != EQUAL {
		t.Fatalf("Expected query to start with equal expression")
	}
	expect := make(TagIDs)
	expect[ids[0]] = struct{}{}
	expect[ids[5]] = struct{}{}
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagFilterByTagMatchWithExpression(t *testing.T) {
	ids := getTestIDs(t)
	q, _ := NewTagQuery([]string{"__tag=~a{1}", "key2=value2"}, 0)
	if q.startWith != EQUAL {
		t.Fatalf("Expected query to start with equal expression")
	}

	expect := make(TagIDs)
	expect[ids[5]] = struct{}{}
	queryAndCompareResults(t, q, expect)

	q, _ = NewTagQuery([]string{"__tag=~a{2}", "key2=value2"}, 0)
	if q.startWith != EQUAL {
		t.Fatalf("Expected query to start with equal expression")
	}
	queryAndCompareResults(t, q, expect)

	q, _ = NewTagQuery([]string{"__tag=~a{3}", "key2=value2"}, 0)
	if q.startWith != EQUAL {
		t.Fatalf("Expected query to start with equal expression")
	}
	queryAndCompareResults(t, q, expect)

	q, _ = NewTagQuery([]string{"__tag=~a{4}", "key2=value2"}, 0)
	if q.startWith != EQUAL {
		t.Fatalf("Expected query to start with equal expression")
	}
	delete(expect, ids[5])
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagFilterByTagPrefixWithExpression(t *testing.T) {
	ids := getTestIDs(t)
	q, _ := NewTagQuery([]string{"__tag^=aa", "key2=value2"}, 0)
	if q.startWith != EQUAL {
		t.Fatalf("Expected query to start with equal expression")
	}

	expect := make(TagIDs)
	expect[ids[5]] = struct{}{}
	queryAndCompareResults(t, q, expect)

	q, _ = NewTagQuery([]string{"__tag^=aaa", "key2=value2"}, 0)
	if q.startWith != EQUAL {
		t.Fatalf("Expected query to start with equal expression")
	}
	queryAndCompareResults(t, q, expect)

	q, _ = NewTagQuery([]string{"__tag^=aaaa", "key2=value2"}, 0)
	if q.startWith != EQUAL {
		t.Fatalf("Expected query to start with equal expression")
	}
	delete(expect, ids[5])
	delete(expect, ids[6])
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagFilterByTagPrefixWithFrom(t *testing.T) {
	ids := getTestIDs(t)
	q, _ := NewTagQuery([]string{"__tag^=aa"}, 7)
	expect := make(TagIDs)
	expect[ids[6]] = struct{}{}
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagFilterByTagPrefixWithDifferentLengths(t *testing.T) {
	ids := getTestIDs(t)
	q, _ := NewTagQuery([]string{"__tag^=a"}, 0)
	expect := make(TagIDs)
	expect[ids[3]] = struct{}{}
	expect[ids[5]] = struct{}{}
	expect[ids[6]] = struct{}{}
	queryAndCompareResults(t, q, expect)

	q, _ = NewTagQuery([]string{"__tag^=aaa"}, 0)
	delete(expect, ids[3])
	queryAndCompareResults(t, q, expect)

	q, _ = NewTagQuery([]string{"__tag^=aaaa"}, 0)
	delete(expect, ids[5])
	delete(expect, ids[6])
	queryAndCompareResults(t, q, expect)
}

func TestQueryByTagFilterByTagPrefixWithEmptyString(t *testing.T) {
	_, err := NewTagQuery([]string{"__tag^="}, 0)
	if err != errInvalidQuery {
		t.Fatalf("Expected an error, but didn't get it")
	}
}

func TestQueryByTagInvalidQuery(t *testing.T) {
	_, err := NewTagQuery([]string{"key!=value1"}, 0)
	if err != errInvalidQuery {
		t.Fatalf("Expected an error, but didn't get it")
	}
}

func TestTagExpressionQueryByTagWithFrom(t *testing.T) {
	tagIdx, byId := getTestIndex(t)

	q, _ := NewTagQuery([]string{"key1=value1"}, 4)
	res := q.Run(tagIdx, byId)
	if len(res) != 1 {
		t.Fatalf("Expected %d results, but got %d", 1, len(res))
	}

	q, _ = NewTagQuery([]string{"key1=value1"}, 3)
	res = q.Run(tagIdx, byId)
	if len(res) != 2 {
		t.Fatalf("Expected %d results, but got %d", 2, len(res))
	}

	q, _ = NewTagQuery([]string{"key1=value1"}, 2)
	res = q.Run(tagIdx, byId)
	if len(res) != 3 {
		t.Fatalf("Expected %d results, but got %d", 3, len(res))
	}

	q, _ = NewTagQuery([]string{"key1=value1"}, 1)
	res = q.Run(tagIdx, byId)
	if len(res) != 4 {
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
		mds[i].Id = ids[i].String()
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

	fullName := func(md schema.MetricData) string {
		name := md.Metric
		sort.Strings(md.Tags)
		for _, tag := range md.Tags {
			name += ";"
			name += tag
		}
		return name
	}

	testCases := []testCase{
		{
			expressions: []string{"key1=value1"},
			expectation: []string{fullName(mds[1]), fullName(mds[11]), fullName(mds[3])},
		}, {
			expressions: []string{"key1=value2"},
			expectation: []string{fullName(mds[18])},
		}, {
			expressions: []string{"key1=~value[0-9]"},
			expectation: []string{fullName(mds[1]), fullName(mds[11]), fullName(mds[18]), fullName(mds[3])},
		}, {
			expressions: []string{"key1=~value[23]"},
			expectation: []string{fullName(mds[18])},
		}, {
			expressions: []string{"key1=value1", "key2=value1"},
			expectation: []string{},
		}, {
			expressions: []string{"key1=value1", "key2=value2"},
			expectation: []string{fullName(mds[1])},
		}, {
			expressions: []string{"key1=~value[12]", "key2=value2"},
			expectation: []string{fullName(mds[1]), fullName(mds[18])},
		}, {
			expressions: []string{"key1=~value1", "key1=value2"},
			expectation: []string{},
		}, {
			expressions: []string{"key1=~value[0-9]", "key2=~", "key3!=value3"},
			expectation: []string{fullName(mds[11])},
		}, {
			expressions: []string{"key2=", "key1=value1"},
			expectation: []string{fullName(mds[11]), fullName(mds[3])},
		},
	}

	for _, tc := range testCases {
		tagQuery, err := NewTagQuery(tc.expressions, 0)
		if err != nil {
			t.Fatalf("Got an unexpected error with query %s: %s", tc.expressions, err)
		}
		res := ix.idsByTagQuery(1, tagQuery)
		if len(tc.expectation) != len(res) {
			t.Fatalf("Result does not match expectation for expressions %+v\nExpected:\n%+v\nGot:\n%+v\n", tc.expressions, tc.expectation, res)
		}

		resPaths := make([]string, 0, len(res))
		for _, r := range res {
			resPaths = append(resPaths, r.Path)
		}
		sort.Strings(tc.expectation)
		sort.Strings(resPaths)
		if !reflect.DeepEqual(resPaths, tc.expectation) {
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
	mds[10].SetId()

	for _, md := range mds {
		ix.AddOrUpdate(md, 1)
	}

	tagQuery, _ := NewTagQuery([]string{"key1=value1", "key2=value2"}, 0)
	res := ix.idsByTagQuery(orgId, tagQuery)

	if len(res) != 1 {
		t.Fatalf("Expected to get 1 result, but got %d", len(res))
	}

	if len(ix.tags[orgId]) != 3 {
		t.Fatalf("Expected tag index to contain 3 keys, but it has %d: %+v", len(ix.tags), ix.tags)
	}

	deleted, err := ix.Delete(orgId, schema.MetricDefinitionFromMetricData(mds[10]).NameWithTags())
	if err != nil {
		t.Fatalf("Error deleting metric: %s", err)
	}

	if len(deleted) != 1 {
		t.Fatalf("Expected 1 metric to get deleted, but got %d", len(deleted))
	}

	res = ix.idsByTagQuery(orgId, tagQuery)

	if len(res) != 0 {
		t.Fatalf("Expected to get 0 results, but got %d", len(res))
	}

	if len(ix.tags[orgId]) > 1 {
		t.Fatalf("Expected tag index to only have one (name) tag, but it has more: %+v", ix.tags)
	}
}

func TestExpressionParsing(t *testing.T) {
	type testCase struct {
		expression string
		key        string
		value      string
		operator   int
		err        error
	}

	testCases := []testCase{
		{
			expression: "key=value",
			key:        "key",
			value:      "value",
			operator:   EQUAL,
			err:        nil,
		}, {
			expression: "key!=",
			key:        "key",
			value:      "",
			operator:   NOT_EQUAL,
			err:        nil,
		}, {
			expression: "key=",
			key:        "key",
			value:      "",
			operator:   EQUAL,
			err:        nil,
		}, {
			expression: "key=~",
			key:        "key",
			value:      "",
			operator:   MATCH,
			err:        nil,
		}, {
			expression: "key=~v_alue",
			key:        "key",
			value:      "v_alue",
			operator:   MATCH,
			err:        nil,
		}, {
			expression: "k!=~v",
			key:        "k",
			value:      "v",
			operator:   NOT_MATCH,
			err:        nil,
		}, {
			expression: "key!!=value",
			err:        errInvalidQuery,
		}, {
			expression: "key==value",
			key:        "key",
			value:      "=value",
			operator:   EQUAL,
			err:        nil,
		}, {
			expression: "key=~=value",
			key:        "key",
			value:      "=value",
			operator:   MATCH,
			err:        nil,
		}, {
			expression: "key",
			err:        errInvalidQuery,
		},
	}

	for _, tc := range testCases {
		expression, err := parseExpression(tc.expression)
		if err != tc.err || (err == nil && (expression.key != tc.key || expression.value != tc.value || expression.operator != tc.operator)) {
			t.Fatalf("Expected the values %s, %s, %d, %q, but got %s, %s, %d, %q", tc.key, tc.value, tc.operator, tc.err, expression.key, expression.value, expression.operator, err)
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
