package schema

import "testing"

func BenchmarkSerializeMetricDataArrayMsgp(b *testing.B) {
	metrics := getDifferentMetricDataArray(b.N)
	b.ResetTimer()
	m := MetricDataArray(metrics)
	data, err := m.MarshalMsg(nil)
	checkErr(b, err)
	b.Logf("with %10d metrics -> final size: %.1f bytes per metric", b.N, float64(len(data))/float64(b.N))
}

func BenchmarkDeSerializeMetricDataArrayMsgp(b *testing.B) {
	metrics := getDifferentMetricDataArray(b.N)
	m := MetricDataArray(metrics)
	data, err := m.MarshalMsg(nil)
	checkErr(b, err)
	var out MetricDataArray
	b.ResetTimer()
	_, err = out.UnmarshalMsg(data)
	checkErr(b, err)
}

func BenchmarkSerializeMetricDataMsgp(b *testing.B) {
	metrics := getDifferentMetricDataArray(b.N)
	b.ResetTimer()
	var data []byte
	var err error
	for _, m := range metrics {
		data, err = m.MarshalMsg(data)
		checkErr(b, err)
	}
	b.Logf("with %10d metrics -> final size: %.1f bytes per metric", b.N, float64(len(data))/float64(b.N))
}

func BenchmarkDeSerializeMetricDataMsgp(b *testing.B) {
	metrics := getDifferentMetricDataArray(b.N)
	var data []byte
	var err error
	for _, m := range metrics {
		data, err = m.MarshalMsg(data)
		checkErr(b, err)
	}
	b.ResetTimer()
	p := &MetricData{}
	for len(data) != 0 {
		data, err = p.UnmarshalMsg(data)
		checkErr(b, err)
	}
}

func BenchmarkSerializeMetricPointMsgp(b *testing.B) {
	metrics := getDifferentMetricPoints(b.N)
	b.ResetTimer()
	var data []byte
	var err error
	for _, m := range metrics {
		data, err = m.MarshalMsg(data)
		checkErr(b, err)
	}
	b.Logf("with %10d metrics -> final size: %.1f bytes per metric", b.N, float64(len(data))/float64(b.N))
}

func BenchmarkDeSerializeMetricPointMsgp(b *testing.B) {
	metrics := getDifferentMetricPoints(b.N)
	var data []byte
	var err error
	for _, m := range metrics {
		data, err = m.MarshalMsg(data)
		checkErr(b, err)
	}
	b.ResetTimer()
	p := &MetricPoint{}
	for len(data) != 0 {
		data, err = p.UnmarshalMsg(data)
		checkErr(b, err)
	}
}

func BenchmarkSerializeMetricPointId1Msgp(b *testing.B) {
	metrics := getDifferentMetricPointId1s(b.N)
	b.ResetTimer()
	var data []byte
	var err error
	for _, m := range metrics {
		data, err = m.MarshalMsg(data)
		checkErr(b, err)
	}
	b.Logf("with %10d metrics -> final size: %.1f bytes per metric", b.N, float64(len(data))/float64(b.N))
}

func BenchmarkDeSerializeMetricPointId1Msgp(b *testing.B) {
	metrics := getDifferentMetricPointId1s(b.N)
	var data []byte
	var err error
	for _, m := range metrics {
		data, err = m.MarshalMsg(data)
		checkErr(b, err)
	}
	b.ResetTimer()
	p := &MetricPointId1{}
	for len(data) != 0 {
		data, err = p.UnmarshalMsg(data)
		checkErr(b, err)
	}
}

func BenchmarkSerializeMetricPointId2Msgp(b *testing.B) {
	metrics := getDifferentMetricPointId2s(b.N)
	b.ResetTimer()
	var data []byte
	var err error
	for _, m := range metrics {
		data, err = m.MarshalMsg(data)
		checkErr(b, err)
	}
	b.Logf("with %10d metrics -> final size: %.1f bytes per metric", b.N, float64(len(data))/float64(b.N))
}

func BenchmarkDeSerializeMetricPointId2Msgp(b *testing.B) {
	metrics := getDifferentMetricPointId2s(b.N)
	var data []byte
	var err error
	for _, m := range metrics {
		data, err = m.MarshalMsg(data)
		checkErr(b, err)
	}
	b.ResetTimer()
	p := &MetricPointId2{}
	for len(data) != 0 {
		data, err = p.UnmarshalMsg(data)
		checkErr(b, err)
	}
}

func BenchmarkSerializeMetricPointId1Manual(b *testing.B) {
	metrics := getDifferentMetricPointId1s(b.N)
	b.ResetTimer()
	var data []byte
	var err error
	for _, m := range metrics {
		data, err = m.MarshalManual(data)
		checkErr(b, err)
	}
	b.Logf("with %10d metrics -> final size: %.1f bytes per metric", b.N, float64(len(data))/float64(b.N))
}

func BenchmarkDeSerializeMetricPointId1Manual(b *testing.B) {
	metrics := getDifferentMetricPointId1s(b.N)
	var data []byte
	var err error
	for _, m := range metrics {
		data, err = m.MarshalManual(data)
		checkErr(b, err)
	}
	b.ResetTimer()
	p := &MetricPointId1{}
	for len(data) != 0 {
		data, err = p.UnmarshalManual(data)
		checkErr(b, err)
	}
}
