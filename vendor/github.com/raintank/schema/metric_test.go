package schema

import (
	"testing"
)

func TestNameWithTagFromMetricData(t *testing.T) {
	md := MetricData{
		Name: "mytestname",
		Tags: []string{"mytag1=myvalue1", "mytag2=myvalue2"},
	}

	expectedNameWithTags := "mytestname;mytag1=myvalue1;mytag2=myvalue2"
	generatedNameWithTags := string(md.KeyBySeriesWithTags(nil))
	if generatedNameWithTags != expectedNameWithTags {
		t.Fatalf("Generated name with tags is not as we expected: %s != %s", expectedNameWithTags, generatedNameWithTags)
	}
}
