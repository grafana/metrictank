package memory

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/grafana/metrictank/idx"

	"github.com/raintank/schema"

	"github.com/grafana/metrictank/expr/tagquery"
)

func TestSelectByMetricTag(t *testing.T) {
	withAndWithoutPartitonedIndex(withAndWithoutTagSupport(withAndWithoutMetaTagSupport(testSelectByMetricTag)))(t)
}

func testSelectByMetricTag(t *testing.T) {
	index, byId := getTestIndex()
	ids := getTestIDs()

	ctx := &TagQueryContext{
		index: index,
		byId:  byId,
	}

	expr, err := tagquery.ParseExpression("key4=value4")
	if err != nil {
		t.Fatalf("Failed to parse expression: %s", err)
	}

	selector := newIdSelector(expr, ctx, false)
	resCh, _ := selector.getIds()
	res := make(IdSet)
	for id := range resCh {
		res[id] = struct{}{}
	}

	expectedRes := IdSet{ids[2]: struct{}{}, ids[6]: struct{}{}}

	if !reflect.DeepEqual(res, expectedRes) {
		t.Fatalf("Result did not match expecteed result:\nExpected: %+v\nGot: %+v\n", expectedRes, res)
	}
}

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

func TestSelectByMetaTag(t *testing.T) {
	_tagSupport := TagSupport
	TagSupport = true
	defer func() { TagSupport = _tagSupport }()
	withAndWithoutMetaTagSupport(testSelectByMetaTag)(t)
}

func testSelectByMetaTag(t *testing.T) {
	index := NewUnpartitionedMemoryIdx()
	defer index.Stop()
	index.Init()

	archives, mds := getTestArchives(10)
	for i := range archives {
		index.add(archives[i])
	}

	expressions, err := tagquery.ParseExpressions([]string{"tag1=~value[3-5]$", "name!=some.id.of.a.metric.4"})
	if err != nil {
		t.Fatalf("Unexpected error when parsing expressions: %s", err)
	}

	index.MetaTagRecordUpsert(1, tagquery.MetaTagRecord{
		MetaTags: []tagquery.Tag{
			{
				Key:   "meta1",
				Value: "value1",
			},
		},
		Expressions: expressions,
	})

	ctx := &TagQueryContext{
		index:       index.tags[1],
		byId:        index.defById,
		mti:         index.metaTags[1],
		metaRecords: index.metaTagRecords[1],
	}

	expr, err := tagquery.ParseExpression("meta1=value1")
	if err != nil {
		t.Fatalf("Failed to parse expression: %s", err)
	}

	selector := newIdSelector(expr, ctx, false)
	resCh, _ := selector.getIds()
	res := make(IdSet)
	for id := range resCh {
		res[id] = struct{}{}
	}

	expectedRes := make(IdSet)
	if tagquery.MetaTagSupport {
		expectedRes = IdSet{mds[3].Id: struct{}{}, mds[5].Id: struct{}{}}
	}

	if !reflect.DeepEqual(res, expectedRes) {
		t.Fatalf("Result did not match expecteed result:\nExpected: %+v\nGot: %+v\n", expectedRes, res)
	}
}
