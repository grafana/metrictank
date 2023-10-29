package schema

import "testing"

func BenchmarkSerializeMetricPointStaticBuffer(b *testing.B) {
	metrics := getDifferentMetricPoints(b.N)
	data := make([]byte, 0, b.N*32)
	b.ResetTimer()
	var err error
	for _, m := range metrics {
		data, err = m.Marshal32(data)
		checkErr(b, err)
	}
	b.Logf("with %10d metrics -> final size: %.1f bytes per metric", b.N, float64(len(data))/float64(b.N))
}

func BenchmarkSerializeMetricPoint(b *testing.B) {
	metrics := getDifferentMetricPoints(b.N)
	b.ResetTimer()
	var data []byte
	var err error
	for _, m := range metrics {
		data, err = m.Marshal(data)
		checkErr(b, err)
	}
	b.Logf("with %10d metrics -> final size: %.1f bytes per metric", b.N, float64(len(data))/float64(b.N))
}

func BenchmarkDeSerializeMetricPoint(b *testing.B) {
	metrics := getDifferentMetricPoints(b.N)
	var data []byte
	var err error
	for _, m := range metrics {
		data, err = m.Marshal(data)
		checkErr(b, err)
	}
	b.ResetTimer()
	p := &MetricPoint{}
	for len(data) != 0 {
		data, err = p.Unmarshal(data)
		checkErr(b, err)
	}
}
