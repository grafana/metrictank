package input

import (
	"fmt"
	"github.com/grafana/metrictank/idx"
	"testing"
	"time"

	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/idx/memory"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/mdata/cache"
	backendStore "github.com/grafana/metrictank/store"
	"github.com/raintank/schema"
)

func TestIngestValidTagAndValueWithRejection(t *testing.T) {
	handler, index, reset := getDefaultHandler(t)
	defer reset()

	rejectInvalidTags = false
	data := getTestMetricData()
	data.Tags = []string{"valid=tag"}
	testIngestMetricData(t, "valid_with_rejection_0", data, handler, index, 0, 0, 1)
}

func TestIngestValidTagAndValueWithoutRejection(t *testing.T) {
	handler, index, reset := getDefaultHandler(t)
	defer reset()

	rejectInvalidTags = true
	data := getTestMetricData()
	data.Tags = []string{"valid=tag"}
	testIngestMetricData(t, "valid_without_rejection_0", data, handler, index, 0, 0, 1)
}

func TestInvalidTagsWithRejection(t *testing.T) {
	handler, index, reset := getDefaultHandler(t)
	defer reset()
	invalidTags := generateInvalidTags(t)

	rejectInvalidTags = true
	data := getTestMetricData()
	for tc, invalidTag := range invalidTags {
		data.Tags = []string{fmt.Sprintf("%s=value", invalidTag)}
		testIngestMetricData(t, fmt.Sprintf("invalid_tags_with_rejection_%d", tc), data, handler, index, 1, 1, 0)
	}
}

func TestIngestInvalidTagsWithoutRejection(t *testing.T) {
	handler, index, reset := getDefaultHandler(t)
	defer reset()
	invalidTags := generateInvalidTags(t)

	rejectInvalidTags = false
	data := getTestMetricData()
	for tc, invalidTag := range invalidTags {
		data.Tags = []string{fmt.Sprintf("%s=value", invalidTag)}
		testIngestMetricData(t, fmt.Sprintf("invalid_tags_without_rejection_%d", tc), data, handler, index, 1, 1, 1)
	}
}

func generateInvalidTags(t *testing.T) []string {
	t.Helper()

	invalidChars := ";!^"
	validChar := "a"

	return generateInvalidStrings(t, invalidChars, validChar)
}

func TestInvalidTagValuesWithRejection(t *testing.T) {
	handler, index, reset := getDefaultHandler(t)
	defer reset()
	invalidTagValues := generateInvalidTagValues(t)

	rejectInvalidTags = true
	data := getTestMetricData()
	for tc, invalidTagValue := range invalidTagValues {
		data.Tags = []string{fmt.Sprintf("tag=%s", invalidTagValue)}
		testIngestMetricData(t, fmt.Sprintf("invalid_tag_values_with_rejection_%d", tc), data, handler, index, 1, 1, 0)
	}
}

func TestIngestInvalidTagValuesWithoutRejection(t *testing.T) {
	handler, index, reset := getDefaultHandler(t)
	defer reset()
	invalidTagValues := generateInvalidTagValues(t)

	rejectInvalidTags = false
	data := getTestMetricData()
	for tc, invalidTagValue := range invalidTagValues {
		data.Tags = []string{fmt.Sprintf("tag=%s", invalidTagValue)}
		testIngestMetricData(t, fmt.Sprintf("invalid_tag_values_without_rejection_%d", tc), data, handler, index, 1, 1, 1)
	}
}

func generateInvalidTagValues(t *testing.T) []string {
	t.Helper()

	invalidChars := ";~"
	validChar := "a"

	return generateInvalidStrings(t, invalidChars, validChar)
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

func testIngestMetricData(t *testing.T, tc string, data schema.MetricData, handler DefaultHandler, index idx.MetricIndex, expectedInvalidMdInc, expectedInvalidTagMdInc, expectedIndexSizeInc uint32) {
	originalInvalidCnt := handler.invalidMD.Peek()
	originalInvalidTagCnt := handler.invalidTagMD.Peek()
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
	metrics := mdata.NewAggMetrics(nil, nil, false, 3600, 7200, 3600)
	return NewDefaultHandler(metrics, index, "test"), index, reset
}

func BenchmarkProcessMetricDataUniqueMetrics(b *testing.B) {
	cluster.Init("default", "test", time.Now(), "http", 6060)

	store := backendStore.NewDevnullStore()

	mdata.SetSingleSchema(conf.NewRetentionMT(10, 10000, 600, 10, 0))
	mdata.SetSingleAgg(conf.Avg, conf.Min, conf.Max)

	aggmetrics := mdata.NewAggMetrics(store, &cache.MockCache{}, false, 800, 8000, 0)
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

	mdata.SetSingleSchema(conf.NewRetentionMT(10, 10000, 600, 10, 0))
	mdata.SetSingleAgg(conf.Avg, conf.Min, conf.Max)

	aggmetrics := mdata.NewAggMetrics(store, &cache.MockCache{}, false, 800, 8000, 0)
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
