package memory

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/grafana/metrictank/idx"

	"github.com/grafana/metrictank/schema"

	"github.com/grafana/metrictank/expr/tagquery"
)

func getTestArchives(count uint32) ([]*idx.Archive, []schema.MetricDefinition) {
	mds := make([]schema.MetricDefinition, count)
	res := make([]*idx.Archive, count)
	for i := range res {
		mds[i] = schema.MetricDefinition{
			Name:     fmt.Sprintf("some.id.of.a.metric.%d", i),
			OrgId:    1,
			Interval: 1,
			Tags:     []string{fmt.Sprintf("tag1=value%d", i), "tag2=other"},
		}
		mds[i].SetId()

		res[i] = createArchive(&mds[i])
	}

	return res, mds
}

func selectAndCompareResults(t *testing.T, expression tagquery.Expression, metaRecords []tagquery.MetaTagRecord, expectedRes IdSet) {
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

	resCh, _ := newIdSelector(expression, ctx).getIds()
	res := make(IdSet)
	for id := range resCh {
		res[id] = struct{}{}
	}

	if !reflect.DeepEqual(res, expectedRes) {
		t.Fatalf("Result does not match expectation:\nExpected:\n%+v\nGot:\n%+v", expectedRes, res)
	}
}

func TestSelectByMetricTag(t *testing.T) {
	_tagSupport := TagSupport
	TagSupport = true
	defer func() { TagSupport = _tagSupport }()
	withAndWithoutPartitonedIndex(withAndWithoutMetaTagSupport(testSelectByMetricTag))(t)
}

func testSelectByMetricTag(t *testing.T) {
	_, mds := getTestArchives(10)

	expr, err := tagquery.ParseExpression("tag1=value4")
	if err != nil {
		t.Fatalf("Failed to parse expression: %s", err)
	}

	expectedRes := IdSet{mds[4].Id: struct{}{}}

	selectAndCompareResults(t, expr, nil, expectedRes)
}

func TestSelectByMetaTag(t *testing.T) {
	_tagSupport := TagSupport
	TagSupport = true
	defer func() { TagSupport = _tagSupport }()
	withAndWithoutPartitonedIndex(withAndWithoutMetaTagSupport(testSelectByMetaTag))(t)
}

func testSelectByMetaTag(t *testing.T) {
	_, mds := getTestArchives(10)

	metaTagExpressions, err := tagquery.ParseExpressions([]string{"tag1=~value[3-5]$", "name!=some.id.of.a.metric.4"})
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

	query, err := tagquery.ParseExpression("meta1=value1")
	if err != nil {
		t.Fatalf("Failed to parse expression: %s", err)
	}

	// with meta tag support disabled we expect an empty result set
	expectedRes := make(IdSet)
	if MetaTagSupport {
		expectedRes = IdSet{mds[3].Id: struct{}{}, mds[5].Id: struct{}{}}
	}

	selectAndCompareResults(t, query, metaRecords, expectedRes)
}

func TestSelectByMetaTagAndMetricTagUsingSameKeyAndValue(t *testing.T) {
	_tagSupport := TagSupport
	TagSupport = true
	defer func() { TagSupport = _tagSupport }()
	withAndWithoutMetaTagSupport(testSelectByMetaTagAndMetricTagUsingSameKeyAndValue)(t)
}

func testSelectByMetaTagAndMetricTagUsingSameKeyAndValue(t *testing.T) {
	_, mds := getTestArchives(10)

	// create a meta tag "tag1=value3" which matches metrics with the metric tag "tag1=value4"
	metaTagExpressions, err := tagquery.ParseExpressions([]string{"tag1=value4"})
	if err != nil {
		t.Fatalf("Unexpected error when parsing expressions: %s", err)
	}
	metaRecords := []tagquery.MetaTagRecord{
		{
			MetaTags: []tagquery.Tag{
				{
					Key:   "tag1",
					Value: "value3",
				},
			},
			Expressions: metaTagExpressions,
		},
	}

	query, err := tagquery.ParseExpression("tag1=value3")
	if err != nil {
		t.Fatalf("Failed to parse expression: %s", err)
	}

	var expectedRes IdSet
	if MetaTagSupport {
		// if meta tag support is enabled we expect mds[3] to get returned based on its metric tag tag1=value3
		// and we also expect mds[4] to get returned based on its associated meta tag tag1=value3
		expectedRes = IdSet{mds[3].Id: struct{}{}, mds[4].Id: struct{}{}}
	} else {
		// if meta tag support is disabled we only expect mds[3] to get returned based on its metric tag tag1=value3
		expectedRes = IdSet{mds[3].Id: struct{}{}}
	}

	selectAndCompareResults(t, query, metaRecords, expectedRes)
}

func TestSelectByMetaTagAndMetricTagUsingDifferentKeyAndValue(t *testing.T) {
	_tagSupport := TagSupport
	TagSupport = true
	defer func() { TagSupport = _tagSupport }()
	withAndWithoutMetaTagSupport(testSelectByMetaTagAndMetricTagUsingDifferentKeyAndValue)(t)
}

func testSelectByMetaTagAndMetricTagUsingDifferentKeyAndValue(t *testing.T) {
	_, mds := getTestArchives(10)

	// create a meta tag "tag1=value2.3" which matches all metrics with the metric tag "tag1=value4"
	metaTagExpressions, err := tagquery.ParseExpressions([]string{"tag1=value4"})
	if err != nil {
		t.Fatalf("Unexpected error when parsing expressions: %s", err)
	}
	metaRecords := []tagquery.MetaTagRecord{
		{
			MetaTags: []tagquery.Tag{
				{
					Key:   "tag1",
					Value: "value2.3",
				},
			},
			Expressions: metaTagExpressions,
		},
	}

	// use query expression which matches both tag1=value3 and also tag1=value2.3
	query, err := tagquery.ParseExpression("tag1=~value[2\\.]*3")
	if err != nil {
		t.Fatalf("Failed to parse expression: %s", err)
	}

	var expectedRes IdSet
	if MetaTagSupport {
		// if meta tag support is enabled we expect mds[3] to get returned based on its metric tag tag1=value2.3
		// and we also expect mds[4] to get returned based on its associated meta tag tag1=value3
		expectedRes = IdSet{mds[3].Id: struct{}{}, mds[4].Id: struct{}{}}
	} else {
		// if meta tag support is disabled we only expect mds[3] to get returned based on its metric tag tag1=value3
		expectedRes = IdSet{mds[3].Id: struct{}{}}
	}

	selectAndCompareResults(t, query, metaRecords, expectedRes)
}

func TestSelectMetaTagWhichConflictsMetricTag(t *testing.T) {
	_tagSupport := TagSupport
	TagSupport = true
	defer func() { TagSupport = _tagSupport }()
	withAndWithoutMetaTagSupport(testSelectMetaTagWhichConflictsMetricTag)(t)
}

func testSelectMetaTagWhichConflictsMetricTag(t *testing.T) {
	_, mds := getTestArchives(10)

	// create a meta tag "tag1=value4", which includes "tag1=value3" and "tag1=value5",
	// but explicitly excludes "tag1=value4"
	metaTagExpressions, err := tagquery.ParseExpressions([]string{"tag1=~value[3-5]$", "tag1!=value4"})
	if err != nil {
		t.Fatalf("Unexpected error when parsing expressions: %s", err)
	}
	metaRecords := []tagquery.MetaTagRecord{
		{
			MetaTags: []tagquery.Tag{
				{
					Key:   "tag1",
					Value: "value4",
				},
			},
			Expressions: metaTagExpressions,
		},
	}

	query, err := tagquery.ParseExpression("tag1=value4")
	if err != nil {
		t.Fatalf("Failed to parse expression: %s", err)
	}

	var expectedRes IdSet
	if MetaTagSupport {
		// if meta tag support is enabled we expect mds[3] to get returned based on its metric tag tag1=value2.3
		// and we also expect mds[4] to get returned based on its associated meta tag tag1=value3
		expectedRes = IdSet{mds[3].Id: struct{}{}, mds[4].Id: struct{}{}, mds[5].Id: struct{}{}}
	} else {
		// if meta tag support is disabled we only expect mds[3] to get returned based on its metric tag tag1=value3
		expectedRes = IdSet{mds[4].Id: struct{}{}}
	}

	selectAndCompareResults(t, query, metaRecords, expectedRes)
}

func TestSelectByMetaTagWhichRefersToItself(t *testing.T) {
	_tagSupport := TagSupport
	TagSupport = true
	defer func() { TagSupport = _tagSupport }()
	withAndWithoutMetaTagSupport(testSelectByMetaTagWhichRefersToItself)(t)
}

func testSelectByMetaTagWhichRefersToItself(t *testing.T) {
	_, mds := getTestArchives(10)

	// create meta record which would select itself and lead to a loop if we didn't
	// have the loop prevention mechanism using the TagQueryContext.subQuery flag
	metaTagExpressions, err := tagquery.ParseExpressions([]string{"tag1=value4"})
	if err != nil {
		t.Fatalf("Unexpected error when parsing expressions: %s", err)
	}
	metaRecords := []tagquery.MetaTagRecord{
		{
			MetaTags: []tagquery.Tag{
				{
					Key:   "tag1",
					Value: "value4",
				},
			},
			Expressions: metaTagExpressions,
		},
	}

	query, err := tagquery.ParseExpression("tag1=value4")
	if err != nil {
		t.Fatalf("Failed to parse expression: %s", err)
	}

	expectedRes := IdSet{mds[4].Id: struct{}{}}

	selectAndCompareResults(t, query, metaRecords, expectedRes)
}
