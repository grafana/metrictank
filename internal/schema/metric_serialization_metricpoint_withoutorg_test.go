package schema

import "testing"

func BenchmarkSerializeMetricPointStaticBufferWithoutOrg(b *testing.B) {
	metrics := getDifferentMetricPoints(b.N)
	data := make([]byte, 0, b.N*28)
	b.ResetTimer()
	var err error
	for _, m := range metrics {
		data, err = m.MarshalWithoutOrg28(data)
		checkErr(b, err)
	}
	b.Logf("with %10d metrics -> final size: %.1f bytes per metric", b.N, float64(len(data))/float64(b.N))
}

func BenchmarkSerializeMetricPointWithoutOrg(b *testing.B) {
	metrics := getDifferentMetricPoints(b.N)
	b.ResetTimer()
	var data []byte
	var err error
	for _, m := range metrics {
		data, err = m.MarshalWithoutOrg(data)
		checkErr(b, err)
	}
	b.Logf("with %10d metrics -> final size: %.1f bytes per metric", b.N, float64(len(data))/float64(b.N))
}

func BenchmarkDeSerializeMetricPointWithoutOrg(b *testing.B) {
	metrics := getDifferentMetricPoints(b.N)
	var data []byte
	var err error
	for _, m := range metrics {
		data, err = m.MarshalWithoutOrg(data)
		checkErr(b, err)
	}
	b.ResetTimer()
	p := &MetricPoint{}
	for len(data) != 0 {
		data, err = p.UnmarshalWithoutOrg(data)
		checkErr(b, err)
	}
}
