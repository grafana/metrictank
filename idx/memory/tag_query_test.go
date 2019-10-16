package memory

import (
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/grafana/metrictank/expr/tagquery"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
)

var ids []schema.MKey

func getTestIDs() []schema.MKey {
	if len(ids) > 0 {
		return ids
	}

	ids = []schema.MKey{
		test.GetMKey(0),
		test.GetMKey(1),
		test.GetMKey(2),
		test.GetMKey(3),
		test.GetMKey(4),
		test.GetMKey(5),
		test.GetMKey(6),
		test.GetMKey(7),
	}

	return ids
}

func getTestIndex() (TagIndex, map[schema.MKey]*idx.Archive) {
	type testCase struct {
		id         schema.MKey
		lastUpdate int64
		tags       []string
	}

	ids := getTestIDs()

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
	byId := make(map[schema.MKey]*idx.Archive)

	for i, d := range data {
		byId[d.id] = &idx.Archive{}
		byId[d.id].Name = fmt.Sprintf("metric%d", i)
		byId[d.id].Tags = d.tags
		byId[d.id].LastUpdate = d.lastUpdate
		for _, tag := range d.tags {
			tagSplits := strings.Split(tag, "=")
			tagIdx.addTagId(tagSplits[0], tagSplits[1], d.id)
		}
		tagIdx.addTagId("name", byId[d.id].Name, d.id)
	}

	return tagIdx, byId
}

func queryAndCompareTagResults(t *testing.T, q TagQueryContext, expectedData map[string]struct{}) {
	t.Helper()
	tagIdx, byId := getTestIndex()

	res := q.RunGetTags(tagIdx, byId, nil, nil)
	if !reflect.DeepEqual(expectedData, res) {
		t.Fatalf("Expected: %+v\nGot: %+v", expectedData, res)
	}
}

func queryAndCompareResults(t *testing.T, q TagQueryContext, expectedData IdSet) {
	t.Helper()
	tagIdx, byId := getTestIndex()

	resCh := make(chan schema.MKey, 100)
	q.RunNonBlocking(tagIdx, byId, nil, nil, resCh)
	res := make(IdSet)
	for id := range resCh {
		res[id] = struct{}{}
	}

	if !reflect.DeepEqual(expectedData, res) {
		t.Fatalf("Returned data does not match expected data:\nExpected: %s\nGot: %s", expectedData, res)
	}
}

func TestIdHasTag(t *testing.T) {
	tagIdx, _ := getTestIndex()

	ids := getTestIDs()
	if tagIdx.idHasTag(ids[1], "key4", "value4") {
		t.Fatalf("Expected false, but got true")
	}
	if !tagIdx.idHasTag(ids[2], "key4", "value4") {
		t.Fatalf("Expected true, but got false")
	}
}

func TestQueryByTagSimpleEqual(t *testing.T) {
	ids := getTestIDs()
	q, _ := tagquery.NewQueryFromStrings([]string{"key1=value1", "key3=value3"}, 0)

	expect := make(IdSet)
	expect[ids[1]] = struct{}{}
	expect[ids[3]] = struct{}{}
	queryAndCompareResults(t, NewTagQueryContext(q), expect)
}

func TestQueryByTagSimplePrefix(t *testing.T) {
	ids := getTestIDs()
	q, _ := tagquery.NewQueryFromStrings([]string{"key3^=val"}, 0)
	expect := make(IdSet)
	expect[ids[1]] = struct{}{}
	expect[ids[3]] = struct{}{}
	expect[ids[4]] = struct{}{}
	expect[ids[6]] = struct{}{}
	expect[ids[7]] = struct{}{}
	queryAndCompareResults(t, NewTagQueryContext(q), expect)
}

func TestQueryByTagPrefixSpecialCaseName(t *testing.T) {
	ids := getTestIDs()
	q, _ := tagquery.NewQueryFromStrings([]string{"key1=value1", "name^=metric2"}, 0)
	expect := make(IdSet)
	expect[ids[2]] = struct{}{}
	queryAndCompareResults(t, NewTagQueryContext(q), expect)
}

func TestQueryByTagFilterByPrefix(t *testing.T) {
	ids := getTestIDs()
	q, _ := tagquery.NewQueryFromStrings([]string{"key1=value1", "key3^=val"}, 0)
	expect := make(IdSet)
	expect[ids[1]] = struct{}{}
	expect[ids[3]] = struct{}{}
	queryAndCompareResults(t, NewTagQueryContext(q), expect)
}

func TestQueryByTagSimplePattern(t *testing.T) {
	ids := getTestIDs()
	q, _ := tagquery.NewQueryFromStrings([]string{"key4=~value[43]", "key3=~value[1-3]"}, 0)
	expect := make(IdSet)
	expect[ids[6]] = struct{}{}
	expect[ids[3]] = struct{}{}
	queryAndCompareResults(t, NewTagQueryContext(q), expect)
}

func TestQueryByTagSimpleUnequal(t *testing.T) {
	ids := getTestIDs()
	q, _ := tagquery.NewQueryFromStrings([]string{"key1=value1", "key4!=value4"}, 0)
	expect := make(IdSet)
	expect[ids[0]] = struct{}{}
	expect[ids[1]] = struct{}{}
	expect[ids[3]] = struct{}{}
	queryAndCompareResults(t, NewTagQueryContext(q), expect)
}

func TestQueryByTagSimpleNotPattern(t *testing.T) {
	ids := getTestIDs()
	q, _ := tagquery.NewQueryFromStrings([]string{"key1=~value?", "key4!=~value[0-9]", "key2!=~va.+"}, 0)
	expect := make(IdSet)
	expect[ids[1]] = struct{}{}
	queryAndCompareResults(t, NewTagQueryContext(q), expect)
}

func TestQueryByTagWithEqualEmpty(t *testing.T) {
	ids := getTestIDs()
	q, _ := tagquery.NewQueryFromStrings([]string{"key1=value1", "key2=", "key2=~"}, 0)
	expect := make(IdSet)
	expect[ids[1]] = struct{}{}
	expect[ids[2]] = struct{}{}
	expect[ids[3]] = struct{}{}
	queryAndCompareResults(t, NewTagQueryContext(q), expect)
}

func TestQueryByTagWithUnequalEmpty(t *testing.T) {
	ids := getTestIDs()
	q, _ := tagquery.NewQueryFromStrings([]string{"key1=value1", "key3!="}, 0)
	expect := make(IdSet)
	expect[ids[1]] = struct{}{}
	expect[ids[3]] = struct{}{}
	queryAndCompareResults(t, NewTagQueryContext(q), expect)
}

func TestQueryByTagNameEquals(t *testing.T) {
	ids := getTestIDs()
	q, _ := tagquery.NewQueryFromStrings([]string{"key1=value1", "key3=value3", "name=metric1"}, 0)
	expect := make(IdSet)
	expect[ids[1]] = struct{}{}
	queryAndCompareResults(t, NewTagQueryContext(q), expect)
}

func TestQueryByTagNameRegex(t *testing.T) {
	ids := getTestIDs()
	q, _ := tagquery.NewQueryFromStrings([]string{"key1=value1", "key3=value3", "name=~metr"}, 0)
	expect := make(IdSet)
	expect[ids[1]] = struct{}{}
	expect[ids[3]] = struct{}{}
	queryAndCompareResults(t, NewTagQueryContext(q), expect)
}

func TestQueryByTagFilterByTagMatch(t *testing.T) {
	q, _ := tagquery.NewQueryFromStrings([]string{"__tag=~a{1}"}, 0)
	expectTags := make(map[string]struct{})
	expectTags["abc"] = struct{}{}
	expectTags["aaa"] = struct{}{}
	queryAndCompareTagResults(t, NewTagQueryContext(q), expectTags)

	q, _ = tagquery.NewQueryFromStrings([]string{"__tag=~a{2}"}, 0)
	delete(expectTags, "abc")
	queryAndCompareTagResults(t, NewTagQueryContext(q), expectTags)

	q, _ = tagquery.NewQueryFromStrings([]string{"__tag=~a{3}"}, 0)
	queryAndCompareTagResults(t, NewTagQueryContext(q), expectTags)

	q, _ = tagquery.NewQueryFromStrings([]string{"__tag=~a{4}"}, 0)
	delete(expectTags, "aaa")
	queryAndCompareTagResults(t, NewTagQueryContext(q), expectTags)
}

func TestQueryByTagFilterByTagPrefix(t *testing.T) {
	q, _ := tagquery.NewQueryFromStrings([]string{"__tag^=a"}, 0)
	expectTags := make(map[string]struct{})
	expectTags["abc"] = struct{}{}
	expectTags["aaa"] = struct{}{}
	queryAndCompareTagResults(t, NewTagQueryContext(q), expectTags)

	q, _ = tagquery.NewQueryFromStrings([]string{"__tag^=aa"}, 0)
	delete(expectTags, "abc")
	queryAndCompareTagResults(t, NewTagQueryContext(q), expectTags)

	q, _ = tagquery.NewQueryFromStrings([]string{"__tag^=aaa"}, 0)
	queryAndCompareTagResults(t, NewTagQueryContext(q), expectTags)

	q, _ = tagquery.NewQueryFromStrings([]string{"__tag^=aaaa"}, 0)
	delete(expectTags, "aaa")
	queryAndCompareTagResults(t, NewTagQueryContext(q), expectTags)
}

func TestQueryByTagFilterByTagPrefixSpecialCaseName(t *testing.T) {
	q, _ := tagquery.NewQueryFromStrings([]string{"key1=value1", "__tag^=na"}, 0)
	expectTags := make(map[string]struct{})
	expectTags["name"] = struct{}{}
	queryAndCompareTagResults(t, NewTagQueryContext(q), expectTags)
}

func TestQueryByTagFilterByTagMatchWithExpressionAndNameException(t *testing.T) {
	ids := getTestIDs()
	q, _ := tagquery.NewQueryFromStrings([]string{"__tag=~na", "key2=value2"}, 0)
	expect := make(IdSet)
	expect[ids[0]] = struct{}{}
	expect[ids[5]] = struct{}{}
	queryAndCompareResults(t, NewTagQueryContext(q), expect)
}

func TestQueryByTagFilterByTagMatchWithExpression(t *testing.T) {
	ids := getTestIDs()
	q, _ := tagquery.NewQueryFromStrings([]string{"__tag=~a{1}", "key2=value2"}, 0)
	expect := make(IdSet)
	expect[ids[5]] = struct{}{}
	queryAndCompareResults(t, NewTagQueryContext(q), expect)

	q, _ = tagquery.NewQueryFromStrings([]string{"__tag=~a{2}", "key2=value2"}, 0)
	queryAndCompareResults(t, NewTagQueryContext(q), expect)

	q, _ = tagquery.NewQueryFromStrings([]string{"__tag=~a{3}", "key2=value2"}, 0)
	queryAndCompareResults(t, NewTagQueryContext(q), expect)

	q, _ = tagquery.NewQueryFromStrings([]string{"__tag=~a{4}", "key2=value2"}, 0)
	delete(expect, ids[5])
	queryAndCompareResults(t, NewTagQueryContext(q), expect)
}

func TestQueryByTagFilterByTagPrefixWithExpression(t *testing.T) {
	ids := getTestIDs()
	q, _ := tagquery.NewQueryFromStrings([]string{"__tag^=aa", "key2=value2"}, 0)

	expect := make(IdSet)
	expect[ids[5]] = struct{}{}
	queryAndCompareResults(t, NewTagQueryContext(q), expect)

	q, _ = tagquery.NewQueryFromStrings([]string{"__tag^=aaa", "key2=value2"}, 0)
	queryAndCompareResults(t, NewTagQueryContext(q), expect)

	q, _ = tagquery.NewQueryFromStrings([]string{"__tag^=aaaa", "key2=value2"}, 0)
	delete(expect, ids[5])
	delete(expect, ids[6])
	queryAndCompareResults(t, NewTagQueryContext(q), expect)
}

func TestQueryByTagFilterByTagPrefixWithFrom(t *testing.T) {
	ids := getTestIDs()
	q, _ := tagquery.NewQueryFromStrings([]string{"__tag^=aa"}, 7)
	expect := make(IdSet)
	expect[ids[6]] = struct{}{}
	queryAndCompareResults(t, NewTagQueryContext(q), expect)
}

func TestQueryByTagFilterByTagPrefixWithDifferentLengths(t *testing.T) {
	ids := getTestIDs()
	q, _ := tagquery.NewQueryFromStrings([]string{"__tag^=a"}, 0)
	expect := make(IdSet)
	expect[ids[3]] = struct{}{}
	expect[ids[5]] = struct{}{}
	expect[ids[6]] = struct{}{}
	queryAndCompareResults(t, NewTagQueryContext(q), expect)

	q, _ = tagquery.NewQueryFromStrings([]string{"__tag^=aaa"}, 0)
	delete(expect, ids[3])
	queryAndCompareResults(t, NewTagQueryContext(q), expect)

	q, _ = tagquery.NewQueryFromStrings([]string{"__tag^=aaaa"}, 0)
	delete(expect, ids[5])
	delete(expect, ids[6])
	queryAndCompareResults(t, NewTagQueryContext(q), expect)
}

func TestTagExpressionQueryByTagWithFrom(t *testing.T) {
	tagIdx, byId := getTestIndex()

	q, _ := tagquery.NewQueryFromStrings([]string{"key1=value1"}, 4)
	qCtx := NewTagQueryContext(q)
	resCh := make(chan schema.MKey, 100)
	qCtx.RunNonBlocking(tagIdx, byId, nil, nil, resCh)
	res := make(IdSet)
	for id := range resCh {
		res[id] = struct{}{}
	}

	if len(res) != 1 {
		t.Fatalf("Expected %d results, but got %d", 1, len(res))
	}

	q, _ = tagquery.NewQueryFromStrings([]string{"key1=value1"}, 3)
	qCtx = NewTagQueryContext(q)
	resCh = make(chan schema.MKey, 100)
	qCtx.RunNonBlocking(tagIdx, byId, nil, nil, resCh)
	res = make(IdSet)
	for id := range resCh {
		res[id] = struct{}{}
	}
	if len(res) != 2 {
		t.Fatalf("Expected %d results, but got %d", 2, len(res))
	}

	q, _ = tagquery.NewQueryFromStrings([]string{"key1=value1"}, 2)
	qCtx = NewTagQueryContext(q)
	resCh = make(chan schema.MKey, 100)
	qCtx.RunNonBlocking(tagIdx, byId, nil, nil, resCh)
	res = make(IdSet)
	for id := range resCh {
		res[id] = struct{}{}
	}
	if len(res) != 3 {
		t.Fatalf("Expected %d results, but got %d", 3, len(res))
	}

	q, _ = tagquery.NewQueryFromStrings([]string{"key1=value1"}, 1)
	qCtx = NewTagQueryContext(q)
	resCh = make(chan schema.MKey, 100)
	qCtx.RunNonBlocking(tagIdx, byId, nil, nil, resCh)
	res = make(IdSet)
	for id := range resCh {
		res[id] = struct{}{}
	}
	if len(res) != 4 {
		t.Fatalf("Expected %d results, but got %d", 4, len(res))
	}
}

func TestTagExpressionQueriesInAllCombinations(t *testing.T) {
	type expressionWithOperator struct {
		expression  string
		operator    tagquery.ExpressionOperator
		expectedIds []int // based on the data generated in getTestIndex
	}
	expressionStrings := []expressionWithOperator{
		{
			expression:  "key1=value1",
			operator:    tagquery.EQUAL,
			expectedIds: []int{0, 1, 2, 3},
		}, {
			expression:  "key3!=value3",
			operator:    tagquery.NOT_EQUAL,
			expectedIds: []int{0, 2, 5, 6, 7, 8},
		}, {
			expression:  "key2=~v",
			operator:    tagquery.MATCH,
			expectedIds: []int{0, 4, 5},
		}, {
			expression:  "key3!=~.*1$",
			operator:    tagquery.NOT_MATCH,
			expectedIds: []int{0, 1, 2, 3, 4, 5, 7},
		}, {
			expression:  "key3^=v",
			operator:    tagquery.PREFIX,
			expectedIds: []int{1, 3, 4, 5, 6, 7},
		}, {
			expression:  "__tag^=a",
			operator:    tagquery.PREFIX_TAG,
			expectedIds: []int{3, 5, 6},
		}, {
			expression:  "__tag=abc",
			operator:    tagquery.HAS_TAG,
			expectedIds: []int{3, 5},
		}, {
			expression:  "key2=",
			operator:    tagquery.NOT_HAS_TAG,
			expectedIds: []int{1, 2, 3, 6, 7},
		}, {
			expression:  "a=~.*",
			operator:    tagquery.MATCH_ALL,
			expectedIds: []int{0, 1, 2, 3, 4, 5, 6, 7},
		}, {
			expression:  "a!=~.*",
			operator:    tagquery.MATCH_NONE,
			expectedIds: []int{},
		},
	}

	// parse all expression strings into expression objects
	var err error
	allExpressions := make(tagquery.Expressions, len(expressionStrings))
	for i, expr := range expressionStrings {
		allExpressions[i], err = tagquery.ParseExpression(expr.expression)
		if err != nil {
			t.Fatalf("Unexpected error when parsing expression %q: %q", expr, err)
		}
		if allExpressions[i].GetOperator() != expr.operator {
			t.Fatalf("Expression was %q supposed to result in operator %d, but got %d", expr.expression, expr.operator, allExpressions[i].GetOperator())
		}
	}

	// find all the possible combinations of query expressions
	all_subsets := make([][]int, 0, int(math.Pow(2, float64(len(expressionStrings))))-1)
	var find_subsets func([]int, []int)
	find_subsets = func(so_far, rest []int) {
		if len(rest) == 0 {
			if len(so_far) > 0 {
				all_subsets = append(all_subsets, so_far)
			}
		} else {
			find_subsets(append(so_far, rest[0]), rest[1:])
			find_subsets(so_far, rest[1:])
		}
	}
	all_ids := make([]int, len(expressionStrings))
	for i := range all_ids {
		all_ids[i] = i
	}
	find_subsets([]int{}, all_ids)

	ids := getTestIDs()
TEST_CASES:
	for tc, expressionIds := range all_subsets {
		expressions := make(tagquery.Expressions, len(expressionIds))
		var expectedResults []int // intersection of expected results of each expression

		includingExpressionRequiringNonEmptyValue := false
		// build the slice of expressions we want to query for and find the
		// expected results for the current combination of expressions
		for i, expressionId := range expressionIds {
			expressions[i] = allExpressions[expressionId]
			includingExpressionRequiringNonEmptyValue = includingExpressionRequiringNonEmptyValue || expressions[i].RequiresNonEmptyValue()

			if i == 0 {
				expectedResults = make([]int, len(expressionStrings[expressionId].expectedIds))
				copy(expectedResults, expressionStrings[expressionId].expectedIds)
			} else {
			EXPECTED_RESULTS:
				for j := 0; j < len(expectedResults); j++ {
					for _, id := range expressionStrings[expressionId].expectedIds {
						if expectedResults[j] == id {
							continue EXPECTED_RESULTS
						}
					}
					expectedResults = append(expectedResults[:j], expectedResults[j+1:]...)
					j--
				}
			}
		}

		// this combination of expressions would result in an invalid query
		// because there is no expression requiring a non-empty value
		if !includingExpressionRequiringNonEmptyValue {
			continue TEST_CASES
		}

		expectedIds := make(IdSet, len(expectedResults))
		for j := range expectedResults {
			expectedIds[ids[expectedResults[j]]] = struct{}{}
		}

		builder := strings.Builder{}
		builder.WriteString(fmt.Sprintf("TC %d: ", tc))
		for _, expr := range expressions {
			expr.StringIntoWriter(&builder)
			builder.WriteString(";")
		}
		t.Run(builder.String(), func(t *testing.T) {
			query, err := tagquery.NewQuery(expressions, 0)
			if err != nil {
				t.Fatalf("Unexpected error when getting query from expressions %q: %q", expressions, err)
			}

			queryAndCompareResults(t, NewTagQueryContext(query), expectedIds)
		})
	}
}

func TestGetByTag(t *testing.T) {
	withAndWithoutPartitonedIndex(testGetByTag)(t)
}

func testGetByTag(t *testing.T) {
	_tagSupport := TagSupport
	defer func() { TagSupport = _tagSupport }()
	TagSupport = true

	ix := New()
	ix.Init()
	defer ix.Stop()

	idString := "1.000000000000000000000000000000%02x"
	mds := make([]schema.MetricData, 20)
	ids := make([]string, 20)
	for i := range mds {
		ids[i] = fmt.Sprintf(idString, i)
		mds[i].Name = fmt.Sprintf("metric.%d", i)
		mds[i].Id = ids[i]
		mds[i].OrgId = 1
		mds[i].Interval = 1
		mds[i].Time = 12345
	}
	mds[1].Tags = []string{"key1=value1", "key2=value2"}
	mds[11].Tags = []string{"key1=value1"}
	mds[18].Tags = []string{"key1=value2", "key2=value2"}
	mds[3].Tags = []string{"key1=value1", "key3=value3"}

	for i, md := range mds {
		mds[i].SetId()
		mkey, err := schema.MKeyFromString(mds[i].Id)
		if err != nil {
			t.Fatal(err)
		}
		ix.AddOrUpdate(mkey, &md, 1)
	}

	type testCase struct {
		expressions []string
		expectation []string
	}

	fullName := func(md schema.MetricData) string {
		name := md.Name
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
			expectation: []string{fullName(mds[1]), fullName(mds[11]), fullName(mds[18])},
		}, {
			expressions: []string{"key2=", "key1=value1"},
			expectation: []string{fullName(mds[11]), fullName(mds[3])},
		},
	}

	for _, tc := range testCases {
		q, err := tagquery.NewQueryFromStrings(tc.expressions, 0)
		if err != nil {
			t.Fatalf("Got an unexpected error with query %s: %s", tc.expressions, err)
		}
		resCh := ix.idsByTagQuery(1, NewTagQueryContext(q))
		res := make(IdSet)
		for id := range resCh {
			res[id] = struct{}{}
		}

		if len(tc.expectation) != len(res) {
			t.Fatalf("Result does not match expectation for expressions %+v\nExpected:\n%+v\nGot:\n%+v\n", tc.expressions, tc.expectation, res)
		}

		resPaths := make([]string, 0, len(res))
		for id := range res {
			def, ok := ix.Get(id)
			if !ok {
				t.Fatalf("Tag query returned ID that did not exist in DefByID: %s", id)
			}
			resPaths = append(resPaths, def.NameWithTags())
		}
		sort.Strings(tc.expectation)
		sort.Strings(resPaths)
		if !reflect.DeepEqual(resPaths, tc.expectation) {
			t.Fatalf("Result does not match expectation\nGot:\n%+v\nExpected:\n%+v\n", res, tc.expectation)
		}
	}
}
