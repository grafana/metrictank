package schema

import (
	"encoding/json"
	"testing"
)

func BenchmarkSerializeMetricDataArrayJson(b *testing.B) {
	metrics := getDifferentMetrics(b.N)
	b.ResetTimer()
	data, err := json.Marshal(metrics)
	checkErr(b, err)
	b.Logf("with %10d metrics -> final size: %.1f bytes per metric", b.N, float64(len(data))/float64(b.N))
}

func BenchmarkDeSerializeMetricDataArrayJson(b *testing.B) {
	metrics := getDifferentMetrics(b.N)
	data, err := json.Marshal(metrics)
	checkErr(b, err)
	var out []*MetricData
	b.ResetTimer()
	err = json.Unmarshal(data, &out)
	checkErr(b, err)
}
