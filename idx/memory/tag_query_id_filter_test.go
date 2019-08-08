package memory

import (
	"testing"

	"github.com/raintank/schema"

	"github.com/grafana/metrictank/expr/tagquery"
)

func filterAndCompareResults(t *testing.T, expressions tagquery.Expressions, metaRecords []tagquery.MetaTagRecord, expectedMatch, expectedFail []schema.MetricDefinition) {
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
		index:       index.tags[1],
		byId:        index.defById,
		mti:         index.metaTags[1],
		metaRecords: index.metaTagRecords[1],
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
		tagquery.MetaTagRecord{
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

	if tagquery.MetaTagSupport {
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
		tagquery.MetaTagRecord{
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

	if tagquery.MetaTagSupport {
		expectedMatch = []schema.MetricDefinition{mds[0], mds[1], mds[2], mds[4], mds[7], mds[8], mds[9]}
		expectedFail = []schema.MetricDefinition{mds[3], mds[5], mds[6]}
	} else {
		expectedMatch = mds
		expectedFail = nil
	}

	filterAndCompareResults(t, tagquery.Expressions{notHasTagExpr}, metaRecords, expectedMatch, expectedFail)
	filterAndCompareResults(t, tagquery.Expressions{notEqualExpr}, metaRecords, expectedMatch, expectedFail)

}
