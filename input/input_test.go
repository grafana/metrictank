package input

import (
	"fmt"
	"testing"
	"time"

	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/idx/memory"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/mdata/cache"
	"github.com/grafana/metrictank/schema"
	backendStore "github.com/grafana/metrictank/store"
)

func TestIngestValidAndInvalidTagsAndValuesWithAndWithoutRejection(t *testing.T) {
	type testCase struct {
		name                    string
		mdName                  string
		rejectInvalidTags       bool
		rejectInvalidUtf8       bool
		tags                    []string
		expectedInvalidMdInc    uint32
		expectedInvalidTagMdInc uint32
		expectedInvalidUtf8Inc  uint32
		expectedIndexSizeInc    uint32
	}

	testCases := []testCase{
		{
			name:                    "valid_utf8_with_rejection",
			mdName:                  "abc",
			rejectInvalidTags:       false,
			rejectInvalidUtf8:       true,
			tags:                    []string{"valid=tag"},
			expectedInvalidMdInc:    0,
			expectedInvalidTagMdInc: 0,
			expectedInvalidUtf8Inc:  0,
			expectedIndexSizeInc:    1,
		}, {
			name:                    "valid_utf8_without_rejection",
			mdName:                  "abc",
			rejectInvalidTags:       false,
			rejectInvalidUtf8:       false,
			tags:                    []string{"valid=tag"},
			expectedInvalidMdInc:    0,
			expectedInvalidTagMdInc: 0,
			expectedInvalidUtf8Inc:  0,
			expectedIndexSizeInc:    1,
		}, {
			name:                    "invalid_utf8_name_with_rejection",
			mdName:                  "abc\xc5",
			rejectInvalidTags:       false,
			rejectInvalidUtf8:       true,
			tags:                    []string{"valid=tag"},
			expectedInvalidMdInc:    1,
			expectedInvalidTagMdInc: 0,
			expectedInvalidUtf8Inc:  1,
			expectedIndexSizeInc:    0,
		}, {
			name:                    "invalid_utf8_name_without_rejection",
			mdName:                  "abc\xc5",
			rejectInvalidTags:       false,
			rejectInvalidUtf8:       false,
			tags:                    []string{"valid=tag"},
			expectedInvalidMdInc:    1,
			expectedInvalidTagMdInc: 0,
			expectedInvalidUtf8Inc:  1,
			expectedIndexSizeInc:    1,
		}, {
			name:                    "invalid_utf8_tag_values_with_rejection",
			mdName:                  "abc",
			rejectInvalidTags:       false,
			rejectInvalidUtf8:       true,
			tags:                    []string{"invalid=bb\xc5"},
			expectedInvalidMdInc:    1,
			expectedInvalidTagMdInc: 0,
			expectedInvalidUtf8Inc:  1,
			expectedIndexSizeInc:    0,
		}, {
			name:                    "invalid_utf8_tag_values_without_rejection",
			mdName:                  "abc",
			rejectInvalidTags:       false,
			rejectInvalidUtf8:       false,
			tags:                    []string{"invalid=bb\xc5"},
			expectedInvalidMdInc:    1,
			expectedInvalidTagMdInc: 0,
			expectedInvalidUtf8Inc:  1,
			expectedIndexSizeInc:    1,
		}, {
			name:                    "invalid_utf8_tag_values_and_invalid_tags_with_rejection",
			mdName:                  "abc",
			rejectInvalidTags:       true,
			rejectInvalidUtf8:       false,
			tags:                    []string{"invalid!!!;;;=bb\xc5@#;;;!@#"},
			expectedInvalidMdInc:    1,
			expectedInvalidTagMdInc: 1,
			expectedInvalidUtf8Inc:  0,
			expectedIndexSizeInc:    0,
		}, {
			name:                    "invalid_utf8_tag_values_and_invalid_tags_with_utf8_rejection",
			mdName:                  "abc",
			rejectInvalidTags:       false,
			rejectInvalidUtf8:       true,
			tags:                    []string{"invalid!!!;;;=bb\xc5@#;;;!@#"},
			expectedInvalidMdInc:    1,
			expectedInvalidTagMdInc: 1,
			expectedInvalidUtf8Inc:  0, // since the tag is invalid we won't ever see invalid UTF8 error
			expectedIndexSizeInc:    1, // index still increases because we are not rejecting invalid tags
		},
		{
			name:                    "invalid_utf8_tag_values_and_invalid_tags_with_utf8_rejection_and_tag_rejection",
			mdName:                  "abc",
			rejectInvalidTags:       true,
			rejectInvalidUtf8:       true,
			tags:                    []string{"invalid!!!;;;=bb\xc5@#;;;!@#"},
			expectedInvalidMdInc:    1,
			expectedInvalidTagMdInc: 1,
			expectedInvalidUtf8Inc:  0, // since the tag is invalid we won't ever see invalid UTF8 error
			expectedIndexSizeInc:    0,
		},
		{
			name:                    "valid_with_rejection",
			mdName:                  "abc",
			rejectInvalidTags:       true,
			rejectInvalidUtf8:       false,
			tags:                    []string{"valid=tag"},
			expectedInvalidMdInc:    0,
			expectedInvalidTagMdInc: 0,
			expectedInvalidUtf8Inc:  0,
			expectedIndexSizeInc:    1,
		}, {
			name:                    "valid_without_rejection",
			mdName:                  "abc",
			rejectInvalidTags:       false,
			rejectInvalidUtf8:       false,
			tags:                    []string{"valid=tag"},
			expectedInvalidMdInc:    0,
			expectedInvalidTagMdInc: 0,
			expectedInvalidUtf8Inc:  0,
			expectedIndexSizeInc:    1,
		}, {
			name:                    "invalid_tags_with_rejection",
			mdName:                  "abc",
			rejectInvalidTags:       true,
			rejectInvalidUtf8:       false,
			tags:                    generateInvalidTags(t),
			expectedInvalidMdInc:    1,
			expectedInvalidTagMdInc: 1,
			expectedInvalidUtf8Inc:  0,
			expectedIndexSizeInc:    0,
		}, {
			name:                    "invalid_tags_without_rejection",
			mdName:                  "abc",
			rejectInvalidTags:       false,
			rejectInvalidUtf8:       false,
			tags:                    generateInvalidTags(t),
			expectedInvalidMdInc:    1,
			expectedInvalidTagMdInc: 1,
			expectedInvalidUtf8Inc:  0,
			expectedIndexSizeInc:    1,
		}, {
			name:                    "invalid_tag_values_with_rejection",
			mdName:                  "abc",
			rejectInvalidTags:       true,
			rejectInvalidUtf8:       false,
			tags:                    generateInvalidTagValues(t),
			expectedInvalidMdInc:    1,
			expectedInvalidTagMdInc: 1,
			expectedInvalidUtf8Inc:  0,
			expectedIndexSizeInc:    0,
		}, {
			name:                    "invalid_tag_values_without_rejection",
			mdName:                  "abc",
			rejectInvalidTags:       false,
			rejectInvalidUtf8:       false,
			tags:                    generateInvalidTagValues(t),
			expectedInvalidMdInc:    1,
			expectedInvalidTagMdInc: 1,
			expectedInvalidUtf8Inc:  0,
			expectedIndexSizeInc:    1,
		},
	}

	for _, tc := range testCases {
		handler, index, reset := getDefaultHandler(t)
		rejectInvalidTags = tc.rejectInvalidTags
		rejectInvalidUtf8 = tc.rejectInvalidUtf8
		for i, tag := range tc.tags {
			data := getTestMetricData()
			data.Tags = []string{tag}
			data.Name = tc.mdName
			testIngestMetricData(
				t,
				fmt.Sprintf("%s_%d", tc.name, i),
				data,
				handler,
				index,
				tc.expectedInvalidMdInc,
				tc.expectedInvalidTagMdInc,
				tc.expectedInvalidUtf8Inc,
				tc.expectedIndexSizeInc,
			)
		}
		reset()
	}
}

func generateInvalidTags(t *testing.T) []string {
	t.Helper()

	invalidChars := ";!^"
	validChar := "a"

	tagKeys := generateInvalidStrings(t, invalidChars, validChar)
	res := make([]string, 0, len(tagKeys))
	for _, tagKey := range tagKeys {
		res = append(res, fmt.Sprintf("%s=value", tagKey))
	}
	return res
}

func generateInvalidTagValues(t *testing.T) []string {
	t.Helper()

	invalidChars := ";"
	validChar := "a"

	tagValues := generateInvalidStrings(t, invalidChars, validChar)
	res := make([]string, 0, len(tagValues))
	for _, tagValue := range tagValues {
		res = append(res, fmt.Sprintf("tag=%s", tagValue))
	}

	res = append(res, fmt.Sprintf("tag=~"))
	res = append(res, fmt.Sprintf("tag=~a"))
	res = append(res, fmt.Sprintf("tag=~aa"))
	return res
}

func generateInvalidStrings(t *testing.T, invalidChars, validChar string) []string {
	t.Helper()

	// * 4 because we generate
	// - one tag which is only the invalid char we currently process
	// - one 3 letter tag which starts with the invalid char
	// - one 3 letter tag which ends with the invalid char
	// - one 3 letter tag where the invalid char is in the middle
	res := make([]string, len(invalidChars)*4)
	for i, invalidChar := range invalidChars {
		invalidCharStr := string(invalidChar)
		res[i*4] = invalidCharStr
		res[i*4+1] = invalidCharStr + validChar + validChar
		res[i*4+2] = validChar + validChar + invalidCharStr
		res[i*4+3] = validChar + invalidCharStr + validChar
	}
	return res
}

func getTestMetricData() schema.MetricData {
	return schema.MetricData{
		Id:       "1.12345678901234567890123456789012",
		OrgId:    1,
		Name:     "abc",
		Interval: 1,
		Value:    2,
		Time:     3,
		Mtype:    "gauge",
	}
}

func testIngestMetricData(t *testing.T, tc string, data schema.MetricData, handler DefaultHandler, index idx.MetricIndex, expectedInvalidMdInc, expectedInvalidTagMdInc, expectedInvalidUtf8MdInc, expectedIndexSizeInc uint32) {
	originalInvalidCnt := handler.invalidMD.Peek()
	originalInvalidTagCnt := handler.invalidTagMD.Peek()
	originalInvalidUtf8Cnt := handler.invalidUtfMD.Peek()
	originalIndexSize := uint32(len(index.List(1)))
	data.SetId()
	handler.ProcessMetricData(&data, 0)

	invalidCnt := handler.invalidMD.Peek()
	if invalidCnt != originalInvalidCnt+expectedInvalidMdInc {
		t.Fatalf("TC %s: Invalid metric counter has not been updated correctly, expected %d, got %d", tc, originalInvalidCnt+expectedInvalidMdInc, invalidCnt)
	}
	invalidTagCnt := handler.invalidTagMD.Peek()

	if invalidTagCnt != originalInvalidTagCnt+expectedInvalidTagMdInc {
		t.Fatalf("TC %s: Invalid tag counter has not been updated correctly, expected %d, got %d", tc, originalInvalidTagCnt+expectedInvalidTagMdInc, invalidTagCnt)
	}

	invalidUtf8Cnt := handler.invalidUtfMD.Peek()

	if invalidUtf8Cnt != originalInvalidUtf8Cnt+expectedInvalidUtf8MdInc {
		t.Fatalf("TC %s: Invalid utf8 counter has not been updated correctly, expected %d, got %d", tc, originalInvalidUtf8Cnt+expectedInvalidUtf8MdInc, invalidUtf8Cnt)
	}

	indexSize := uint32(len(index.List(1)))
	if indexSize != originalIndexSize+expectedIndexSizeInc {
		t.Fatalf("TC %s: Index size has not been updated correctly, expected %d, got %d", tc, originalIndexSize+expectedIndexSizeInc, indexSize)
	}
}

func getDefaultHandler(t *testing.T) (DefaultHandler, idx.MetricIndex, func()) {
	t.Helper()

	oldRejectInvalidTags := rejectInvalidTags
	oldSchemas := mdata.Schemas
	oldTagSupport := memory.TagSupport
	memory.TagSupport = true
	index := memory.New()

	reset := func() {
		rejectInvalidTags = oldRejectInvalidTags
		mdata.Schemas = oldSchemas
		memory.TagSupport = oldTagSupport
		index.Stop()
	}

	mdata.Schemas = conf.NewSchemas(nil)
	metrics := mdata.NewAggMetrics(nil, nil, false, nil, 3600, 7200, 3600)
	return NewDefaultHandler(metrics, index, "test"), index, reset
}

func BenchmarkProcessMetricDataUniqueMetrics(b *testing.B) {
	cluster.Init("default", "test", time.Now(), "http", 6060)

	store := backendStore.NewDevnullStore()

	mdata.SetSingleSchema(conf.MustParseRetentions("10s:10000s:10min:10:true"))
	mdata.SetSingleAgg(conf.Avg, conf.Min, conf.Max)

	aggmetrics := mdata.NewAggMetrics(store, &cache.MockCache{}, false, nil, 800, 8000, 0)
	metricIndex := memory.New()
	metricIndex.Init()
	defer metricIndex.Stop()

	in := NewDefaultHandler(aggmetrics, metricIndex, "BenchmarkProcess")

	// timestamps start at 10 and go up from there. (we can't use 0, see AggMetric.Add())
	datas := make([]*schema.MetricData, b.N)
	for i := 0; i < b.N; i++ {
		name := fmt.Sprintf("fake.metric.%d", i)
		metric := &schema.MetricData{
			Id:       "1.12345678901234567890123456789012",
			OrgId:    500,
			Name:     name,
			Interval: 10,
			Value:    1234.567,
			Unit:     "ms",
			Time:     int64((i + 1) * 10),
			Mtype:    "gauge",
			Tags:     []string{"some=tag", "ok=yes"},
		}
		datas[i] = metric
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		in.ProcessMetricData(datas[i], 1)
	}
}

func BenchmarkProcessMetricDataSameMetric(b *testing.B) {
	cluster.Init("default", "test", time.Now(), "http", 6060)

	store := backendStore.NewDevnullStore()

	mdata.SetSingleSchema(conf.MustParseRetentions("10s:10000s:10min:10:true"))
	mdata.SetSingleAgg(conf.Avg, conf.Min, conf.Max)

	aggmetrics := mdata.NewAggMetrics(store, &cache.MockCache{}, false, nil, 800, 8000, 0)
	metricIndex := memory.New()
	metricIndex.Init()
	defer metricIndex.Stop()
	in := NewDefaultHandler(aggmetrics, metricIndex, "BenchmarkProcess")

	// timestamps start at 10 and go up from there. (we can't use 0, see AggMetric.Add())
	datas := make([]*schema.MetricData, b.N)
	for i := 0; i < b.N; i++ {
		name := "fake.metric.same"
		metric := &schema.MetricData{
			Id:       "1.12345678901234567890123456789012",
			OrgId:    500,
			Name:     name,
			Interval: 10,
			Value:    1234.567,
			Unit:     "ms",
			Time:     int64((i + 1) * 10),
			Mtype:    "gauge",
			Tags:     []string{"some=tag", "ok=yes"},
		}
		datas[i] = metric
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		in.ProcessMetricData(datas[i], 1)
	}
}
