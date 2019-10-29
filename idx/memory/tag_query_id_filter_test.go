package memory

import (
	"testing"

	"github.com/grafana/metrictank/schema"

	"github.com/grafana/metrictank/expr/tagquery"
)

func filterAndCompareResults(t *testing.T, expressions tagquery.Expressions, metaRecords []tagquery.MetaTagRecord, expectedMatch, expectedFail []schema.MetricDefinition) {
	t.Helper()
	index := NewUnpartitionedMemoryIdx()
	defer index.Stop()
	index.Init()

	archives, _ := getTestArchives(10)
	for i := range archives {
		index.add(archives[i])
	}

	for i := range metaRecords {
		index.MetaTagRecordUpsert(1, metaRecords[i])
	}

	ctx := &TagQueryContext{
		index:          index.tags[1],
		byId:           index.defById,
		metaTagIndex:   index.metaTagIndex[1],
		metaTagRecords: index.metaTagRecords[1],
	}

	filter := newIdFilter(expressions, ctx)

	for _, md := range expectedFail {
		if filter.matches(md.Id, md.Name, md.Tags) {
			t.Fatalf("Expected metric %+v to fail, but it did not", md)
		}
	}

	for _, md := range expectedMatch {
		if !filter.matches(md.Id, md.Name, md.Tags) {
			t.Fatalf("Expected metric %+v to match, but it did not", md)
		}
	}
}

func TestFilterByMetricTag(t *testing.T) {
	withAndWithoutPartitonedIndex(withAndWithoutMetaTagSupport(testFilterByMetricTag))(t)
}

func testFilterByMetricTag(t *testing.T) {
	expr, err := tagquery.ParseExpression("tag1=value3")
	if err != nil {
		t.Fatalf("Failed to parse expression: %s", err)
	}

	_, mds := getTestArchives(10)

	expectedMatch := []schema.MetricDefinition{mds[3]}
	expectedFail := append(mds[:3], mds[4:]...)

	filterAndCompareResults(t, tagquery.Expressions{expr}, nil, expectedMatch, expectedFail)
}

func TestFilterByMetaTagWithEqual(t *testing.T) {
	withAndWithoutPartitonedIndex(withAndWithoutMetaTagSupport(testFilterByMetaTagWithEqual))(t)
}

func testFilterByMetaTagWithEqual(t *testing.T) {
	expr, err := tagquery.ParseExpression("meta1=value1")
	if err != nil {
		t.Fatalf("Failed to parse expression: %s", err)
	}

	metaTagExpressions, err := tagquery.ParseExpressions([]string{"tag1=~value[3-6]$", "name!=some.id.of.a.metric.4"})
	if err != nil {
		t.Fatalf("Unexpected error when parsing expressions: %s", err)
	}

	metaRecords := []tagquery.MetaTagRecord{
		{
			MetaTags: []tagquery.Tag{
				{
					Key:   "meta1",
					Value: "value1",
				},
			},
			Expressions: metaTagExpressions,
		},
	}

	_, mds := getTestArchives(10)

	if MetaTagSupport {
		expectedMatch := []schema.MetricDefinition{mds[3], mds[5], mds[6]}
		expectedFail := append(append(mds[:3], mds[4]), mds[7:]...)

		filterAndCompareResults(t, tagquery.Expressions{expr}, metaRecords, expectedMatch, expectedFail)
	} else {
		expectedFail := mds

		filterAndCompareResults(t, tagquery.Expressions{expr}, metaRecords, nil, expectedFail)
	}
}

func TestFilterByMetaTagWithNotEqualAndWithNotHasTag(t *testing.T) {
	withAndWithoutPartitonedIndex(withAndWithoutMetaTagSupport(testFilterByMetaTagWithNotEqualAndWithNotHasTag))(t)
}

func testFilterByMetaTagWithNotEqualAndWithNotHasTag(t *testing.T) {
	// matches ids 3, 5, 6
	metaTagExpressions, err := tagquery.ParseExpressions([]string{"tag1=~value[3-6]$", "name!=some.id.of.a.metric.4"})
	if err != nil {
		t.Fatalf("Unexpected error when parsing expressions: %s", err)
	}

	metaRecords := []tagquery.MetaTagRecord{
		{
			MetaTags: []tagquery.Tag{
				{
					Key:   "meta1",
					Value: "value1",
				},
			},
			Expressions: metaTagExpressions,
		},
	}

	_, mds := getTestArchives(10)

	notEqualExpr, err := tagquery.ParseExpression("meta1!=value1")
	if err != nil {
		t.Fatalf("Failed to parse expression: %s", err)
	}

	notHasTagExpr, err := tagquery.ParseExpression("meta1=")
	if err != nil {
		t.Fatalf("Failed to parse expression: %s", err)
	}

	var expectedMatch []schema.MetricDefinition
	var expectedFail []schema.MetricDefinition

	if MetaTagSupport {
		expectedMatch = []schema.MetricDefinition{mds[0], mds[1], mds[2], mds[4], mds[7], mds[8], mds[9]}
		expectedFail = []schema.MetricDefinition{mds[3], mds[5], mds[6]}
	} else {
		expectedMatch = mds
		expectedFail = nil
	}

	filterAndCompareResults(t, tagquery.Expressions{notHasTagExpr}, metaRecords, expectedMatch, expectedFail)
	filterAndCompareResults(t, tagquery.Expressions{notEqualExpr}, metaRecords, expectedMatch, expectedFail)
}

func TestFilterByMetaTagWithEqualAndWithHasTag(t *testing.T) {
	withAndWithoutPartitonedIndex(withAndWithoutMetaTagSupport(testFilterByMetaTagWithEqualAndWithHasTag))(t)
}

func testFilterByMetaTagWithEqualAndWithHasTag(t *testing.T) {
	// matches ids 3, 5, 6
	metaTagExpressions, err := tagquery.ParseExpressions([]string{"name=~some.id.of.a.metric.[3-6]$", "tag1!=value4"})
	if err != nil {
		t.Fatalf("Unexpected error when parsing expressions: %s", err)
	}

	metaRecords := []tagquery.MetaTagRecord{
		{
			MetaTags: []tagquery.Tag{
				{
					Key:   "meta1",
					Value: "value1",
				},
			},
			Expressions: metaTagExpressions,
		},
	}

	_, mds := getTestArchives(10)

	notEqualExpr, err := tagquery.ParseExpression("meta1=value1")
	if err != nil {
		t.Fatalf("Failed to parse expression: %s", err)
	}

	notHasTagExpr, err := tagquery.ParseExpression("meta1!=")
	if err != nil {
		t.Fatalf("Failed to parse expression: %s", err)
	}

	var expectedMatch []schema.MetricDefinition
	var expectedFail []schema.MetricDefinition

	if MetaTagSupport {
		expectedMatch = []schema.MetricDefinition{mds[3], mds[5], mds[6]}
		expectedFail = []schema.MetricDefinition{mds[0], mds[1], mds[2], mds[4], mds[7], mds[8], mds[9]}
	} else {
		expectedMatch = nil
		expectedFail = mds
	}

	filterAndCompareResults(t, tagquery.Expressions{notHasTagExpr}, metaRecords, expectedMatch, expectedFail)
	filterAndCompareResults(t, tagquery.Expressions{notEqualExpr}, metaRecords, expectedMatch, expectedFail)
}

func TestFilterByMetaTagWithPatternMatching(t *testing.T) {
	withAndWithoutPartitonedIndex(withAndWithoutMetaTagSupport(testFilterByMetaTagWithPatternMatching))(t)
}

func testFilterByMetaTagWithPatternMatching(t *testing.T) {
	metaTag1Expressions, err := tagquery.ParseExpressions([]string{"tag1=value1"})
	if err != nil {
		t.Fatalf("Unexpected error when parsing expressions: %s", err)
	}
	metaTag2Expressions, err := tagquery.ParseExpressions([]string{"tag1=value2"})
	if err != nil {
		t.Fatalf("Unexpected error when parsing expressions: %s", err)
	}
	metaTag3Expressions, err := tagquery.ParseExpressions([]string{"tag1=value3"})
	if err != nil {
		t.Fatalf("Unexpected error when parsing expressions: %s", err)
	}

	metaRecords := []tagquery.MetaTagRecord{
		{
			MetaTags: []tagquery.Tag{
				{
					Key:   "meta1",
					Value: "value1",
				},
			},
			Expressions: metaTag1Expressions,
		},
		{
			MetaTags: []tagquery.Tag{
				{
					Key:   "meta1",
					Value: "value2",
				},
			},
			Expressions: metaTag2Expressions,
		},
		{
			MetaTags: []tagquery.Tag{
				{
					Key:   "meta1",
					Value: "value3",
				},
			},
			Expressions: metaTag3Expressions,
		},
	}

	_, mds := getTestArchives(10)

	// both of these expressions match ids 2 & 3
	exprByMatch, err := tagquery.ParseExpression("meta1=~value[2-3]$")
	if err != nil {
		t.Fatalf("Failed to parse expression: %s", err)
	}
	exprsByNotEqual, err := tagquery.ParseExpressions([]string{"meta1!=", "meta1!=value1"})
	if err != nil {
		t.Fatalf("Failed to parse expression: %s", err)
	}

	var expectedMatch []schema.MetricDefinition
	var expectedFail []schema.MetricDefinition

	if MetaTagSupport {
		expectedMatch = []schema.MetricDefinition{mds[2], mds[3]}
		expectedFail = append(mds[:2], mds[4:]...)
	} else {
		expectedMatch = nil
		expectedFail = mds
	}

	filterAndCompareResults(t, tagquery.Expressions{exprByMatch}, metaRecords, expectedMatch, expectedFail)
	filterAndCompareResults(t, exprsByNotEqual, metaRecords, expectedMatch, expectedFail)
}

func TestFilterByMetaTagWithTagOperators(t *testing.T) {
	withAndWithoutPartitonedIndex(withAndWithoutMetaTagSupport(testFilterByMetaTagWithTagOperators))(t)
}

func testFilterByMetaTagWithTagOperators(t *testing.T) {
	// matches ids 7 & 9
	metaTagExpressions, err := tagquery.ParseExpressions([]string{"tag1=~value[7-9]$", "tag1!=value8"})
	if err != nil {
		t.Fatalf("Unexpected error when parsing expressions: %s", err)
	}

	metaRecords := []tagquery.MetaTagRecord{
		{
			MetaTags: []tagquery.Tag{
				{
					Key:   "meta1",
					Value: "value1",
				},
			},
			Expressions: metaTagExpressions,
		},
	}

	_, mds := getTestArchives(10)

	tagPrefixExpr, err := tagquery.ParseExpression("__tag^=met")
	if err != nil {
		t.Fatalf("Failed to parse expression: %s", err)
	}

	tagMatchExpr, err := tagquery.ParseExpression("__tag=~.*eta[0-9]$")
	if err != nil {
		t.Fatalf("Failed to parse expression: %s", err)
	}

	hasTagExpr1, err := tagquery.ParseExpression("__tag=~meta1")
	if err != nil {
		t.Fatalf("Failed to parse expression: %s", err)
	}

	hasTagExpr2, err := tagquery.ParseExpression("meta1!=")
	if err != nil {
		t.Fatalf("Failed to parse expression: %s", err)
	}

	var expectedMatch []schema.MetricDefinition
	var expectedFail []schema.MetricDefinition

	if MetaTagSupport {
		expectedMatch = []schema.MetricDefinition{mds[7], mds[9]}
		expectedFail = []schema.MetricDefinition{mds[0], mds[1], mds[2], mds[3], mds[4], mds[5], mds[6], mds[8]}
	} else {
		expectedMatch = nil
		expectedFail = mds
	}

	filterAndCompareResults(t, tagquery.Expressions{tagPrefixExpr}, metaRecords, expectedMatch, expectedFail)
	filterAndCompareResults(t, tagquery.Expressions{tagMatchExpr}, metaRecords, expectedMatch, expectedFail)
	filterAndCompareResults(t, tagquery.Expressions{hasTagExpr1}, metaRecords, expectedMatch, expectedFail)
	filterAndCompareResults(t, tagquery.Expressions{hasTagExpr2}, metaRecords, expectedMatch, expectedFail)
}

func TestFilterByMultipleOfManyMetaTagValues(t *testing.T) {
	withAndWithoutPartitonedIndex(withAndWithoutMetaTagSupport(testFilterByMultipleOfManyMetaTagValues))(t)
}

func testFilterByMultipleOfManyMetaTagValues(t *testing.T) {
	// matches ids 1, 2, 3
	metaTagExpressions1, err := tagquery.ParseExpressions([]string{"tag1=~value[1-3]$"})
	if err != nil {
		t.Fatalf("Unexpected error when parsing expressions: %s", err)
	}
	// matches ids 2, 3, 4
	metaTagExpressions2, err := tagquery.ParseExpressions([]string{"tag1=~value[2-4]$"})
	if err != nil {
		t.Fatalf("Unexpected error when parsing expressions: %s", err)
	}
	// matches ids 3, 4, 5
	metaTagExpressions3, err := tagquery.ParseExpressions([]string{"tag1=~value[3-5]$"})
	if err != nil {
		t.Fatalf("Unexpected error when parsing expressions: %s", err)
	}
	// matches ids 4, 5, 6
	metaTagExpressions4, err := tagquery.ParseExpressions([]string{"tag1=~value[4-6]$"})
	if err != nil {
		t.Fatalf("Unexpected error when parsing expressions: %s", err)
	}

	metaRecords := []tagquery.MetaTagRecord{
		{
			MetaTags: []tagquery.Tag{
				{
					Key:   "meta1",
					Value: "value1",
				},
			},
			Expressions: metaTagExpressions1,
		}, {
			MetaTags: []tagquery.Tag{
				{
					Key:   "meta1",
					Value: "value2",
				},
			},
			Expressions: metaTagExpressions2,
		}, {
			MetaTags: []tagquery.Tag{
				{
					Key:   "meta1",
					Value: "value3",
				},
			},
			Expressions: metaTagExpressions3,
		}, {
			MetaTags: []tagquery.Tag{
				{
					Key:   "meta1",
					Value: "value4",
				},
			},
			Expressions: metaTagExpressions4,
		},
	}

	_, mds := getTestArchives(10)

	matchValue2And3Expr, err := tagquery.ParseExpression("meta1=~value[2-3]")
	if err != nil {
		t.Fatalf("Failed to parse expression: %s", err)
	}

	var expectedMatch []schema.MetricDefinition
	var expectedFail []schema.MetricDefinition

	if MetaTagSupport {
		expectedMatch = []schema.MetricDefinition{mds[2], mds[3], mds[4], mds[5]}
		expectedFail = []schema.MetricDefinition{mds[0], mds[1], mds[6], mds[7], mds[8], mds[9]}
	} else {
		expectedMatch = nil
		expectedFail = mds
	}

	filterAndCompareResults(t, tagquery.Expressions{matchValue2And3Expr}, metaRecords, expectedMatch, expectedFail)
}

func TestFilterByMultipleOfManyMetaTags(t *testing.T) {
	withAndWithoutPartitonedIndex(withAndWithoutMetaTagSupport(testFilterByMultipleOfManyMetaTags))(t)
}

func testFilterByMultipleOfManyMetaTags(t *testing.T) {
	// matches ids 1, 2, 3
	metaTagExpressions1, err := tagquery.ParseExpressions([]string{"tag1=~value[1-3]$"})
	if err != nil {
		t.Fatalf("Unexpected error when parsing expressions: %s", err)
	}
	// matches ids 2, 3, 4
	metaTagExpressions2, err := tagquery.ParseExpressions([]string{"tag1=~value[2-4]$"})
	if err != nil {
		t.Fatalf("Unexpected error when parsing expressions: %s", err)
	}
	// matches ids 3, 4, 5
	metaTagExpressions3, err := tagquery.ParseExpressions([]string{"tag1=~value[3-5]$"})
	if err != nil {
		t.Fatalf("Unexpected error when parsing expressions: %s", err)
	}
	// matches ids 4, 5, 6
	metaTagExpressions4, err := tagquery.ParseExpressions([]string{"tag1=~value[4-6]$"})
	if err != nil {
		t.Fatalf("Unexpected error when parsing expressions: %s", err)
	}

	metaRecords := []tagquery.MetaTagRecord{
		{
			MetaTags: []tagquery.Tag{
				{
					Key:   "meta1",
					Value: "value",
				},
			},
			Expressions: metaTagExpressions1,
		}, {
			MetaTags: []tagquery.Tag{
				{
					Key:   "meta2",
					Value: "value",
				},
			},
			Expressions: metaTagExpressions2,
		}, {
			MetaTags: []tagquery.Tag{
				{
					Key:   "meta3",
					Value: "value",
				},
			},
			Expressions: metaTagExpressions3,
		}, {
			MetaTags: []tagquery.Tag{
				{
					Key:   "meta4",
					Value: "value",
				},
			},
			Expressions: metaTagExpressions4,
		},
	}

	_, mds := getTestArchives(10)

	// matches (2, 3, 4) + (3, 4, 5) - (4, 5, 6) = (2, 3)
	tagqueryExpressions, err := tagquery.ParseExpressions([]string{"__tag=~meta[23]$", "meta4="})
	if err != nil {
		t.Fatalf("Failed to parse expression: %s", err)
	}

	var expectedMatch []schema.MetricDefinition
	var expectedFail []schema.MetricDefinition

	if MetaTagSupport {
		expectedMatch = []schema.MetricDefinition{mds[2], mds[3]}
		expectedFail = []schema.MetricDefinition{mds[0], mds[1], mds[4], mds[5], mds[6], mds[7], mds[8], mds[9]}
	} else {
		expectedMatch = nil
		expectedFail = mds
	}

	filterAndCompareResults(t, tagqueryExpressions, metaRecords, expectedMatch, expectedFail)
}

func TestFilterByOverlappingMetaTags(t *testing.T) {
	withAndWithoutPartitonedIndex(withAndWithoutMetaTagSupport(testFilterByOverlappingMetaTags))(t)
}

func testFilterByOverlappingMetaTags(t *testing.T) {
	// matches ids 1, 2, 3
	metaTagExpressions1, err := tagquery.ParseExpressions([]string{"tag1=~value[1-3]$"})
	if err != nil {
		t.Fatalf("Unexpected error when parsing expressions: %s", err)
	}
	// matches ids 2, 3, 4
	metaTagExpressions2, err := tagquery.ParseExpressions([]string{"tag1=~value[2-4]$"})
	if err != nil {
		t.Fatalf("Unexpected error when parsing expressions: %s", err)
	}

	metaRecords := []tagquery.MetaTagRecord{
		{
			MetaTags: []tagquery.Tag{
				{
					Key:   "meta1",
					Value: "value1",
				},
			},
			Expressions: metaTagExpressions1,
		}, {
			MetaTags: []tagquery.Tag{
				{
					Key:   "meta2",
					Value: "value2",
				},
			},
			Expressions: metaTagExpressions2,
		},
	}

	_, mds := getTestArchives(10)

	// matches (1, 2, 3) - (2, 3, 4) = (1)
	tagqueryExpr1, err := tagquery.ParseExpressions([]string{"meta1=value1", "meta2="})
	if err != nil {
		t.Fatalf("Failed to parse expression: %s", err)
	}

	// same as above
	tagqueryExpr2, err := tagquery.ParseExpressions([]string{"meta1=value1", "meta2!=value2"})
	if err != nil {
		t.Fatalf("Failed to parse expression: %s", err)
	}

	var expectedMatch []schema.MetricDefinition
	var expectedFail []schema.MetricDefinition

	if MetaTagSupport {
		expectedMatch = []schema.MetricDefinition{mds[1]}
		expectedFail = append(mds[2:], mds[0])
	} else {
		expectedMatch = nil
		expectedFail = mds
	}

	filterAndCompareResults(t, tagqueryExpr1, metaRecords, expectedMatch, expectedFail)
	filterAndCompareResults(t, tagqueryExpr2, metaRecords, expectedMatch, expectedFail)
}

func TestFilterSubtractingMetricTagsFromMetaTag(t *testing.T) {
	withAndWithoutPartitonedIndex(withAndWithoutMetaTagSupport(testFilterSubtractingMetricTagsFromMetaTag))(t)
}

func testFilterSubtractingMetricTagsFromMetaTag(t *testing.T) {
	// matches ids 1, 2, 3, 4, 5
	metaTagExpressions, err := tagquery.ParseExpressions([]string{"tag1=~value[1-5]$"})
	if err != nil {
		t.Fatalf("Unexpected error when parsing expressions: %s", err)
	}

	metaRecords := []tagquery.MetaTagRecord{
		{
			MetaTags: []tagquery.Tag{
				{
					Key:   "meta1",
					Value: "value1",
				},
			},
			Expressions: metaTagExpressions,
		},
	}

	_, mds := getTestArchives(10)

	// matches (1, 2, 3, 4, 5) - (2, 4) = (1, 3, 5)
	tagqueryExpr1, err := tagquery.ParseExpressions([]string{"meta1=value1", "tag1!=value2", "tag1!=value4"})
	if err != nil {
		t.Fatalf("Failed to parse expression: %s", err)
	}

	// same as above
	tagqueryExpr2, err := tagquery.ParseExpressions([]string{"meta1=value1", "tag1!=~^value[24]$"})
	if err != nil {
		t.Fatalf("Failed to parse expression: %s", err)
	}

	var expectedMatch []schema.MetricDefinition
	var expectedFail []schema.MetricDefinition

	if MetaTagSupport {
		expectedMatch = []schema.MetricDefinition{mds[1], mds[3], mds[5]}
		expectedFail = []schema.MetricDefinition{mds[0], mds[2], mds[4], mds[6], mds[7], mds[8], mds[9]}
	} else {
		expectedMatch = nil
		expectedFail = mds
	}

	filterAndCompareResults(t, tagqueryExpr1, metaRecords, expectedMatch, expectedFail)
	filterAndCompareResults(t, tagqueryExpr2, metaRecords, expectedMatch, expectedFail)
}
