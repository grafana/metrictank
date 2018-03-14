package schema

import "testing"

func BenchmarkSerializeMetricPointId1DirectStaticBuffer(b *testing.B) {
	metrics := getDifferentMetricPointId1s(b.N)
	data := make([]byte, 0, b.N*28)
	b.ResetTimer()
	var err error
	for _, m := range metrics {
		data, err = m.MarshalDirect(data)
		checkErr(b, err)
	}
	b.Logf("with %10d metrics -> final size: %.1f bytes per metric", b.N, float64(len(data))/float64(b.N))
}

func BenchmarkSerializeMetricPointId1Direct(b *testing.B) {
	metrics := getDifferentMetricPointId1s(b.N)
	b.ResetTimer()
	var data []byte
	var err error
	for _, m := range metrics {
		data, err = m.MarshalDirect(data)
		checkErr(b, err)
	}
	b.Logf("with %10d metrics -> final size: %.1f bytes per metric", b.N, float64(len(data))/float64(b.N))
}

func BenchmarkDeSerializeMetricPointId1Direct(b *testing.B) {
	metrics := getDifferentMetricPointId1s(b.N)
	var data []byte
	var err error
	for _, m := range metrics {
		data, err = m.MarshalDirect(data)
		checkErr(b, err)
	}
	b.ResetTimer()
	p := &MetricPointId1{}
	for len(data) != 0 {
		data, err = p.UnmarshalDirect(data)
		checkErr(b, err)
	}
}
