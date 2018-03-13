package schema

import "testing"

func BenchmarkSerializeMetricDataArrayMsgp(b *testing.B) {
	metrics := getDifferentMetrics(b.N)
	b.ResetTimer()
	m := MetricDataArray(metrics)
	data, err := m.MarshalMsg(nil)
	checkErr(b, err)
	b.Logf("with %10d metrics -> final size: %.1f bytes per metric", b.N, float64(len(data))/float64(b.N))
}

func BenchmarkDeSerializeMetricDataArrayMsgp(b *testing.B) {
	metrics := getDifferentMetrics(b.N)
	m := MetricDataArray(metrics)
	data, err := m.MarshalMsg(nil)
	checkErr(b, err)
	var out MetricDataArray
	b.ResetTimer()
	_, err = out.UnmarshalMsg(data)
	checkErr(b, err)
}
