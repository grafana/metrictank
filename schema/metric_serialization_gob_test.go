package schema

import (
	"bytes"
	"encoding/gob"
	"testing"
)

func BenchmarkSerializeMetricDataArrayGob(b *testing.B) {
	metrics := getDifferentMetricDataArray(b.N)
	b.ResetTimer()
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(metrics)
	checkErr(b, err)
	b.Logf("with %10d metrics -> final size: %.1f bytes per metric", b.N, float64(buf.Len())/float64(b.N))
}
func BenchmarkDeSerializeMetricDataArrayGob(b *testing.B) {
	metrics := getDifferentMetricDataArray(b.N)
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(metrics)
	checkErr(b, err)
	var out []*MetricData
	b.ResetTimer()
	dec := gob.NewDecoder(&buf)
	err = dec.Decode(&out)
	checkErr(b, err)
}
