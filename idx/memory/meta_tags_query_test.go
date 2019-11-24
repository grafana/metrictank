package memory

import (
	"fmt"
	"reflect"
	"testing"

	"github.com/grafana/metrictank/expr/tagquery"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/schema"
)

func getTestIndexWithMetaTags(t testing.TB, metaTags []tagquery.MetaTagRecord, count uint32, idGen func(uint32) string) (*UnpartitionedMemoryIdx, []schema.MKey) {
	t.Helper()
	idx := NewUnpartitionedMemoryIdx()

	var md schema.MetricData
	mkeys := make([]schema.MKey, count)
	for i := uint32(0); i < count; i++ {
		md.Name = "test.name"
		md.OrgId = 1
		md.Interval = 1
		md.Value = 1
		md.Time = 1
		md.Tags = []string{fmt.Sprintf("tag1=iterator%d", i), fmt.Sprintf("tag2=%d", i+1)}
		md.SetId()

		if idGen != nil {
			md.Tags = append(md.Tags, idGen(i))
		}

		mkey, err := schema.MKeyFromString(md.Id)
		if err != nil {
			t.Fatalf("Unexpected error when getting mkey from string %s: %s", md.Id, err)
		}
		idx.AddOrUpdate(mkey, &md, 1)
		mkeys[i] = mkey
	}

	idx.MetaTagRecordSwap(1, metaTags)

	waitForMetaTagEnrichers(t, idx)

	return idx, mkeys
}

func queryAndCompareResultsWithMetaTags(t *testing.T, idx *UnpartitionedMemoryIdx, expressions tagquery.Expressions, expectedData IdSet) {
	t.Helper()

	query, err := tagquery.NewQuery(expressions, 0)
	if err != nil {
		t.Fatalf("Unexpected error when instantiating query from expressions %q: %s", expressions, err)
	}

	res := idx.FindByTag(1, query)

	// extract schema.MKeys from returned result
	resData := make(IdSet, len(res))
	for i := range res {
		if len(res[i].Defs) == 0 {
			continue
		}
		resData[res[i].Defs[0].Id] = struct{}{}
	}

	if len(resData) != len(expectedData) {
		t.Fatalf("Expected data set had different length than received data set: %d / %d", len(expectedData), len(res))
	}

	if !reflect.DeepEqual(resData, expectedData) {
		t.Fatalf("Expected data is different from received data:\nExpected:\n%+v\nReceived:\n%+v\n", expectedData, resData)
	}
}

func TestSimpleMetaTagQueryWithSingleEqualExpression(t *testing.T) {
	reset := enableMetaTagSupport()
	defer reset()

	metaTagRecord, err := tagquery.ParseMetaTagRecord([]string{"metatag1=value1"}, []string{"tag1=iterator3"})
	if err != nil {
		t.Fatalf("Error when parsing meta tag record: %s", err)
	}

	idx, mkeys := getTestIndexWithMetaTags(t, []tagquery.MetaTagRecord{metaTagRecord}, 10, nil)

	expressions, err := tagquery.ParseExpressions([]string{"metatag1=value1"})
	if err != nil {
		t.Fatalf("Error when parsing expressions: %s", err)
	}

	queryAndCompareResultsWithMetaTags(t, idx, expressions, IdSet{mkeys[3]: struct{}{}})
}

func TestSimpleMetaTagQueryWithMatchAndUnequalExpression(t *testing.T) {
	reset := enableMetaTagSupport()
	defer reset()

	metaTagRecord, err := tagquery.ParseMetaTagRecord([]string{"metatag1=value1"}, []string{"tag1=~iterator[3-4]", "tag1!=iterator3"})
	if err != nil {
		t.Fatalf("Error when parsing meta tag record: %s", err)
	}

	idx, mkeys := getTestIndexWithMetaTags(t, []tagquery.MetaTagRecord{metaTagRecord}, 10, nil)

	expressions, err := tagquery.ParseExpressions([]string{"metatag1=value1"})
	if err != nil {
		t.Fatalf("Error when parsing expressions: %s", err)
	}

	queryAndCompareResultsWithMetaTags(t, idx, expressions, IdSet{mkeys[4]: struct{}{}})
}

func TestSimpleMetaTagQueryWithMatchAndNotMatchExpression(t *testing.T) {
	reset := enableMetaTagSupport()
	defer reset()

	metaTagRecord, err := tagquery.ParseMetaTagRecord([]string{"metatag1=value1"}, []string{"tag1=~iterator[3-9]", "tag1!=~iterator[4-8]"})
	if err != nil {
		t.Fatalf("Error when parsing meta tag record: %s", err)
	}

	idx, mkeys := getTestIndexWithMetaTags(t, []tagquery.MetaTagRecord{metaTagRecord}, 10, nil)

	expressions, err := tagquery.ParseExpressions([]string{"metatag1=value1"})
	if err != nil {
		t.Fatalf("Error when parsing expressions: %s", err)
	}

	queryAndCompareResultsWithMetaTags(t, idx, expressions, IdSet{mkeys[3]: struct{}{}, mkeys[9]: struct{}{}})
}

func TestSimpleMetaTagQueryWithManyTypesOfExpression(t *testing.T) {
	reset := enableMetaTagSupport()
	defer reset()

	metaTagRecord, err := tagquery.ParseMetaTagRecord(
		[]string{"metatag1=value1"},
		[]string{"__tag^=tag", "tag1=~iterator[2-9]", "tag1!=~iterator[0-3]", "tag2!=6", "tag2!=~.*8", "name=test.name"},
	)
	if err != nil {
		t.Fatalf("Error when parsing meta tag record: %s", err)
	}

	idx, mkeys := getTestIndexWithMetaTags(t, []tagquery.MetaTagRecord{metaTagRecord}, 10, nil)

	expressions, err := tagquery.ParseExpressions([]string{"metatag1=value1"})
	if err != nil {
		t.Fatalf("Error when parsing expressions: %s", err)
	}

	queryAndCompareResultsWithMetaTags(t, idx, expressions, IdSet{
		mkeys[4]: struct{}{},
		mkeys[6]: struct{}{},
		mkeys[8]: struct{}{},
		mkeys[9]: struct{}{},
	})
}

func TestMetaTagEnrichmentForQueryByMetricTag(t *testing.T) {
	reset := enableMetaTagSupport()
	defer reset()

	metaTagRecord, err := tagquery.ParseMetaTagRecord(
		[]string{"metatag1=value1"},
		[]string{"tag1=~iterator[1-2]"},
	)
	if err != nil {
		t.Fatalf("Error when parsing meta tag record: %s", err)
	}

	idx, _ := getTestIndexWithMetaTags(t, []tagquery.MetaTagRecord{metaTagRecord}, 10, nil)

	expressions, err := tagquery.ParseExpressions([]string{"tag1=~.+"})
	if err != nil {
		t.Fatalf("Error when parsing expressions: %s", err)
	}

	query, err := tagquery.NewQuery(expressions, 0)
	if err != nil {
		t.Fatalf("Unexpected error when instantiating query from expressions %q: %s", expressions, err)
	}

	res := idx.FindByTag(1, query)

	for _, node := range res {
		for _, def := range node.Defs {
			shouldHaveMetaTag := false

			// determine whether this serie should have the meta tag metatag1=value1
			for _, tag := range def.Tags {
				if tag == "tag1=iterator1" || tag == "tag1=iterator2" {
					shouldHaveMetaTag = true
				}
			}

			foundMetaTag := false
			for _, tag := range node.MetaTags {
				if tag == metaTagRecord.MetaTags[0] {
					foundMetaTag = true
				}
			}

			if shouldHaveMetaTag != foundMetaTag {
				if shouldHaveMetaTag {
					t.Fatalf("Expected meta tag, but it wasn't present in: %+v", node)
				} else {
					t.Fatalf("Unexpected meta tag in: %+v", node)
				}
			}
		}
	}
}

func TestMetaTagEnrichmentForQueryByMetaTag(t *testing.T) {
	reset := enableMetaTagSupport()
	defer reset()

	metaTagRecord1, err := tagquery.ParseMetaTagRecord(
		[]string{"metatag1=value1"},
		[]string{"name=~.+", "tag1!=iterator1", "tag1!=iterator2"},
	)
	if err != nil {
		t.Fatalf("Error when parsing meta tag record: %s", err)
	}
	metaTagRecord2, err := tagquery.ParseMetaTagRecord(
		[]string{"metatag1=value2"},
		[]string{"name=~.+", "tag1!=iterator3", "tag1!=iterator4"},
	)
	if err != nil {
		t.Fatalf("Error when parsing meta tag record: %s", err)
	}

	idx, _ := getTestIndexWithMetaTags(t, []tagquery.MetaTagRecord{metaTagRecord1, metaTagRecord2}, 10, nil)

	expressions, err := tagquery.ParseExpressions([]string{"tag1=~.+"})
	if err != nil {
		t.Fatalf("Error when parsing expressions: %s", err)
	}

	query, err := tagquery.NewQuery(expressions, 0)
	if err != nil {
		t.Fatalf("Unexpected error when instantiating query from expressions %q: %s", expressions, err)
	}

	res := idx.FindByTag(1, query)

	for _, node := range res {
		for _, def := range node.Defs {
			shouldHaveMetaTag1 := true
			shouldHaveMetaTag2 := true

			// determine whether this serie should have metatag1=value1 and/or metatag1=value2
			for _, tag := range def.Tags {
				if tag == "tag1=iterator1" || tag == "tag1=iterator2" {
					shouldHaveMetaTag1 = false
				}
				if tag == "tag1=iterator3" || tag == "tag1=iterator4" {
					shouldHaveMetaTag2 = false
				}
			}

			foundMetaTag1 := false
			foundMetaTag2 := false
			for _, tag := range node.MetaTags {
				if tag == metaTagRecord1.MetaTags[0] {
					foundMetaTag1 = true
				}
				if tag == metaTagRecord2.MetaTags[0] {
					foundMetaTag2 = true
				}
			}

			if shouldHaveMetaTag1 != foundMetaTag1 {
				if shouldHaveMetaTag1 {
					t.Fatalf("Expected meta tag metatag1=value1, but it wasn't present in: %+v", def)
				} else {
					t.Fatalf("Unexpected meta tag metatag1=value1 in: %+v", def)
				}
			}
			if shouldHaveMetaTag2 != foundMetaTag2 {
				if shouldHaveMetaTag2 {
					t.Fatalf("Expected meta tag metatag1=value2, but it wasn't present in: %+v", def)
				} else {
					t.Fatalf("Unexpected meta tag metatag1=value2 in: %+v", def)
				}
			}
		}
	}
}

func BenchmarkMetaTagEnricher(b *testing.B) {
	reset := enableMetaTagSupport()
	defer reset()

	metaTagRecords1 := make([]tagquery.MetaTagRecord, 1000)
	for i := 0; i < 1000; i++ {
		metaTagRecords1[i] = parseMetaTagRecordMustCompile(
			b,
			[]string{fmt.Sprintf("metatag1=value%d", i)},
			[]string{fmt.Sprintf("tag1=~.*or%d$", i)},
		)
	}

	metaTagRecords2 := make([]tagquery.MetaTagRecord, 1000)
	for i := 0; i < 1000; i++ {
		metaTagRecords2[i] = parseMetaTagRecordMustCompile(
			b,
			[]string{fmt.Sprintf("metatag2=value%d", i)},
			[]string{fmt.Sprintf("tag1=iterator%d", i)},
		)
	}

	metaTagRecords3 := make([]tagquery.MetaTagRecord, 1000)
	for i := 0; i < 1000; i++ {
		metaTagRecords3[i] = parseMetaTagRecordMustCompile(
			b,
			[]string{fmt.Sprintf("metatag3=value%d", i)},
			[]string{"name=test.name", fmt.Sprintf("tag2=%d", i+1)},
		)
	}

	allMetaTagRecords := make([]tagquery.MetaTagRecord, len(metaTagRecords1)+len(metaTagRecords2)+len(metaTagRecords3))
	cursor := 0
	for i := 0; i < len(metaTagRecords1); i++ {
		allMetaTagRecords[cursor] = metaTagRecords1[i]
		cursor++
	}
	for i := 0; i < len(metaTagRecords2); i++ {
		allMetaTagRecords[cursor] = metaTagRecords2[i]
		cursor++
	}
	for i := 0; i < len(metaTagRecords3); i++ {
		allMetaTagRecords[cursor] = metaTagRecords3[i]
		cursor++
	}

	memoryIdx, keys := getTestIndexWithMetaTags(b, allMetaTagRecords, 1000, nil)

	defs := make([]idx.Archive, len(keys))
	i := 0
	for _, key := range keys {
		defs[i] = *memoryIdx.defById[key]
		i++
	}

	var def *idx.Archive
	resToCompare := make(map[tagquery.Tag]struct{})
	metaTagIdx := memoryIdx.getOrgMetaTagIndex(1)
	b.ReportAllocs()
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		def = &defs[i%1000]
		metaTags := metaTagIdx.getMetaTagsById(def.Id.Key)
		if len(metaTags) != 3 {
			b.Fatalf("Expected result to have length 3, but it had %d", len(metaTags))
		}

		for i := range metaTags {
			resToCompare[metaTags[i]] = struct{}{}
		}

		if len(resToCompare) != 3 {
			b.Fatalf("Expected length %d, but got length %d", 3, len(resToCompare))
		}

		if _, ok := resToCompare[metaTagRecords1[i%1000].MetaTags[0]]; !ok {
			b.Fatalf("Did not find expected tag: %+v", metaTagRecords1[i%1000].MetaTags[0])
		}
		if _, ok := resToCompare[metaTagRecords2[i%1000].MetaTags[0]]; !ok {
			b.Fatalf("Did not find expected tag: %+v", metaTagRecords2[i%1000].MetaTags[0])
		}
		if _, ok := resToCompare[metaTagRecords3[i%1000].MetaTags[0]]; !ok {
			b.Fatalf("Did not find expected tag: %+v", metaTagRecords3[i%1000].MetaTags[0])
		}

		for k := range resToCompare {
			delete(resToCompare, k)
		}
	}
}

func BenchmarkFindByMetaTagIndexSize100kMetaRecordCount200(b *testing.B) {
	benchmarkFindByMetaTag(b, 100000, 200)
}

func BenchmarkFindByMetaTagIndexSize1mMetaRecordCount1000(b *testing.B) {
	benchmarkFindByMetaTag(b, 1000000, 1000)
}

func BenchmarkFindByMetaTagIndexSize1mMetaRecordCount10000(b *testing.B) {
	benchmarkFindByMetaTag(b, 1000000, 10000)
}

// getMetaRecordsForMetaTagQueryBenchmark generates the given number of meta records it assumes that
// the index they get applied to has at least <count> host=hostname? tags
func getMetaRecordsForMetaTagQueryBenchmark(b *testing.B, count int, metaTags [][]string, tagGen func(uint32) string) []tagquery.MetaTagRecord {
	metaRecords := make([]tagquery.MetaTagRecord, count)
	cursor := 0
	var err error
	for i := range metaRecords {
		metaRecords[i], err = tagquery.ParseMetaTagRecord(
			metaTags[cursor%len(metaTags)],
			[]string{tagGen(uint32(i))},
		)
		cursor++
		if err != nil {
			b.Fatalf("Failed to parse meta record: %s", err.Error())
		}
	}

	return metaRecords
}

func getQueriesForMetaTagQueryBenchmark(b *testing.B, count int, queryGen func(uint32) []string) []tagquery.Query {
	queries := make([]tagquery.Query, count)
	var err error
	for i := 0; i < 2; i++ {
		queries[i], err = tagquery.NewQueryFromStrings(queryGen(uint32(i)), 0)
		if err != nil {
			b.Fatalf("Error when parsing query: %s", err.Error())
		}
	}
	return queries
}

func benchmarkFindByMetaTag(b *testing.B, indexSize, metaRecordCount int) {
	reset := enableMetaTagSupport()
	defer reset()

	metaTagSets := [][]string{
		{"dc=datacenter1", "operatingSystem=ubuntu", "stage=prod"},
		{"dc=datacenter2", "operatingSystem=ubuntu", "stage=prod"},
	}
	tagGen := func(id uint32) string {
		return fmt.Sprintf("host=hostname%d", id%uint32(metaRecordCount))
	}
	metaRecords := getMetaRecordsForMetaTagQueryBenchmark(b, metaRecordCount, metaTagSets, tagGen)
	index, _ := getTestIndexWithMetaTags(b, metaRecords, uint32(indexSize), tagGen)

	queryGen := func(id uint32) []string {
		return []string{fmt.Sprintf("dc=datacenter%d", (id%uint32(len(metaRecords)))+1)}
	}
	queries := getQueriesForMetaTagQueryBenchmark(b, 2, queryGen)

	var res []idx.Node
	expectedResCount := indexSize / len(metaTagSets)
	expectedTagsPerDef := 3
	expectedMetaTagsPerDef := 3

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		res = index.FindByTag(1, queries[i%len(queries)])
		if len(res) != expectedResCount {
			b.Fatalf("Unexpected result. Expected %d items, got %d", expectedResCount, len(res))
		}
		if len(res[0].Defs[0].Tags) != expectedTagsPerDef {
			b.Fatalf("Unexpected number of tags in result. Expected %d, got %d", expectedTagsPerDef, len(res[0].Defs[0].Tags))
		}
		if len(res[0].MetaTags) != expectedMetaTagsPerDef {
			b.Fatalf("Unexpected number of meta tags in result. Expected %d, got %d", expectedMetaTagsPerDef, len(res[0].MetaTags))
		}
	}
}

func BenchmarkAddingMetricsToEnricher1kMetaRecords(b *testing.B) {
	benchmarkAddingMetricToEnricher(b, 1000)
}

func BenchmarkAddingMetricsToEnricher10kMetaRecords(b *testing.B) {
	benchmarkAddingMetricToEnricher(b, 10000)
}

func benchmarkAddingMetricToEnricher(b *testing.B, metaRecordCount int) {
	reset := enableMetaTagSupport()
	defer reset()

	metaTagSets := [][]string{
		{"dc=datacenter1", "operatingSystem=ubuntu", "stage=prod"},
		{"dc=datacenter2", "operatingSystem=ubuntu", "stage=prod"},
	}
	tagGen := func(id uint32) string {
		return fmt.Sprintf("host=hostname%d", id%uint32(metaRecordCount))
	}
	metaRecords := getMetaRecordsForMetaTagQueryBenchmark(b, metaRecordCount, metaTagSets, tagGen)
	index, _ := getTestIndexWithMetaTags(b, metaRecords, 0, tagGen)

	var err error
	mkeys := make([]schema.MKey, b.N)
	mds := make([]schema.MetricData, b.N)
	for i := range mds {
		mds[i].Name = "test.name"
		mds[i].OrgId = 1
		mds[i].Interval = 1
		mds[i].Value = 1
		mds[i].Time = 1
		mds[i].Tags = []string{fmt.Sprintf("tag1=iterator%d", i), fmt.Sprintf("tag2=%d", i+1)}
		mds[i].Tags = append(mds[i].Tags, tagGen(uint32(i)))
		mds[i].SetId()

		mkeys[i], err = schema.MKeyFromString(mds[i].Id)
		if err != nil {
			b.Fatalf("Error when parsing id string: %s", mds[i].Id)
		}
	}

	enricher := index.getOrgMetaTagIndex(1).enricher
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		index.AddOrUpdate(mkeys[i], &mds[i], 0)
	}
	enricher.stop()
}
