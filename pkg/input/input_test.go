package input

import (
	"fmt"
	"testing"
	"time"

	"github.com/grafana/metrictank/pkg/cluster"
	"github.com/grafana/metrictank/pkg/conf"
	"github.com/grafana/metrictank/pkg/idx"
	"github.com/grafana/metrictank/pkg/idx/memory"
	"github.com/grafana/metrictank/pkg/mdata"
	"github.com/grafana/metrictank/pkg/mdata/cache"
	"github.com/grafana/metrictank/pkg/schema"
	backendStore "github.com/grafana/metrictank/pkg/store"
)

func TestIngestValidAndInvalidTagsAndValuesWithAndWithoutRejection(t *testing.T) {
	type testCase struct {
		name                      string
		mdName                    string
		rejectInvalidInput        bool
		tags                      []string
		expectedInvalidMdInc      uint32
		expectedInvalidInputMdInc uint32
		expectedIndexSizeInc      uint32
	}

	testCases := []testCase{
		{
			name:                      "valid_utf8_with_rejection",
			mdName:                    "abc",
			rejectInvalidInput:        true,
			tags:                      []string{"valid=tag"},
			expectedInvalidMdInc:      0,
			expectedInvalidInputMdInc: 0,
			expectedIndexSizeInc:      1,
		}, {
			name:                      "valid_utf8_without_rejection",
			mdName:                    "abc",
			rejectInvalidInput:        false,
			tags:                      []string{"valid=tag"},
			expectedInvalidMdInc:      0,
			expectedInvalidInputMdInc: 0,
			expectedIndexSizeInc:      1,
		}, {
			name:                      "invalid_utf8_name_with_rejection",
			mdName:                    "abc\xc5",
			rejectInvalidInput:        true,
			tags:                      []string{"valid=tag"},
			expectedInvalidMdInc:      1,
			expectedInvalidInputMdInc: 1,
			expectedIndexSizeInc:      0,
		}, {
			name:                      "invalid_utf8_name_without_rejection",
			mdName:                    "abc\xc5",
			rejectInvalidInput:        false,
			tags:                      []string{"valid=tag"},
			expectedInvalidMdInc:      1,
			expectedInvalidInputMdInc: 1,
			expectedIndexSizeInc:      1,
		}, {
			name:                      "invalid_utf8_tag_values_with_rejection",
			mdName:                    "abc",
			rejectInvalidInput:        true,
			tags:                      []string{"invalid=bb\xc5"},
			expectedInvalidMdInc:      1,
			expectedInvalidInputMdInc: 1,
			expectedIndexSizeInc:      0,
		}, {
			name:                      "invalid_utf8_tag_values_without_rejection",
			mdName:                    "abc",
			rejectInvalidInput:        false,
			tags:                      []string{"invalid=bb\xc5"},
			expectedInvalidMdInc:      1,
			expectedInvalidInputMdInc: 1,
			expectedIndexSizeInc:      1,
		}, {
			name:                      "invalid_utf8_tag_values_and_invalid_tags_with_rejection",
			mdName:                    "abc",
			rejectInvalidInput:        true,
			tags:                      []string{"invalid!!!;;;=bb\xc5@#;;;!@#"},
			expectedInvalidMdInc:      1,
			expectedInvalidInputMdInc: 1,
			expectedIndexSizeInc:      0,
		}, {
			name:                      "invalid_utf8_tag_values_and_invalid_tags_with_utf8_rejection",
			mdName:                    "abc",
			rejectInvalidInput:        true,
			tags:                      []string{"invalid!!!;;;=bb\xc5@#;;;!@#"},
			expectedInvalidMdInc:      1,
			expectedInvalidInputMdInc: 1,
			expectedIndexSizeInc:      0,
		},
		{
			name:                      "valid_with_rejection",
			mdName:                    "abc",
			rejectInvalidInput:        true,
			tags:                      []string{"valid=tag"},
			expectedInvalidMdInc:      0,
			expectedInvalidInputMdInc: 0,
			expectedIndexSizeInc:      1,
		}, {
			name:                      "valid_without_rejection",
			mdName:                    "abc",
			rejectInvalidInput:        false,
			tags:                      []string{"valid=tag"},
			expectedInvalidMdInc:      0,
			expectedInvalidInputMdInc: 0,
			expectedIndexSizeInc:      1,
		}, {
			name:                      "invalid_tags_with_rejection",
			mdName:                    "abc",
			rejectInvalidInput:        true,
			tags:                      generateInvalidTags(t),
			expectedInvalidMdInc:      1,
			expectedInvalidInputMdInc: 1,
			expectedIndexSizeInc:      0,
		}, {
			name:                      "invalid_tags_without_rejection",
			mdName:                    "abc",
			rejectInvalidInput:        false,
			tags:                      generateInvalidTags(t),
			expectedInvalidMdInc:      1,
			expectedInvalidInputMdInc: 1,
			expectedIndexSizeInc:      1,
		}, {
			name:                      "invalid_tag_values_with_rejection",
			mdName:                    "abc",
			rejectInvalidInput:        true,
			tags:                      generateInvalidTagValues(t),
			expectedInvalidMdInc:      1,
			expectedInvalidInputMdInc: 1,
			expectedIndexSizeInc:      0,
		}, {
			name:                      "invalid_tag_values_without_rejection",
			mdName:                    "abc",
			rejectInvalidInput:        false,
			tags:                      generateInvalidTagValues(t),
			expectedInvalidMdInc:      1,
			expectedInvalidInputMdInc: 1,
			expectedIndexSizeInc:      1,
		},
	}

	for _, tc := range testCases {
		handler, index, reset := getDefaultHandler(t)
		rejectInvalidInput = tc.rejectInvalidInput
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
				tc.expectedInvalidInputMdInc,
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

func testIngestMetricData(t *testing.T, tc string, data schema.MetricData, handler DefaultHandler, index idx.MetricIndex, expectedInvalidMdInc, expectedInvalidInputMdInc, expectedIndexSizeInc uint32) {
	originalInvalidCnt := handler.invalidMD.Peek()
	originalInvalidInputCnt := handler.invalidInputMD.Peek()
	originalIndexSize := uint32(len(index.List(1)))
	data.SetId()
	handler.ProcessMetricData(&data, 0)

	invalidCnt := handler.invalidMD.Peek()
	if invalidCnt != originalInvalidCnt+expectedInvalidMdInc {
		t.Fatalf("TC %s: Invalid metric counter has not been updated correctly, expected %d, got %d", tc, originalInvalidCnt+expectedInvalidMdInc, invalidCnt)
	}
	invalidInputCnt := handler.invalidInputMD.Peek()

	if invalidInputCnt != originalInvalidInputCnt+expectedInvalidInputMdInc {
		t.Fatalf("TC %s: Invalid input counter has not been updated correctly, expected %d, got %d", tc, originalInvalidInputCnt+expectedInvalidInputMdInc, invalidInputCnt)
	}

	indexSize := uint32(len(index.List(1)))
	if indexSize != originalIndexSize+expectedIndexSizeInc {
		t.Fatalf("TC %s: Index size has not been updated correctly, expected %d, got %d", tc, originalIndexSize+expectedIndexSizeInc, indexSize)
	}
}

func getDefaultHandler(t *testing.T) (DefaultHandler, idx.MetricIndex, func()) {
	t.Helper()

	oldrejectInvalidInput := rejectInvalidInput
	oldSchemas := mdata.Schemas
	oldTagSupport := memory.TagSupport
	memory.TagSupport = true
	index := memory.New()

	reset := func() {
		rejectInvalidInput = oldrejectInvalidInput
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
