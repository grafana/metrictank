package cache

import (
	"testing"

	"github.com/grafana/metrictank/mdata/chunk"
	"gopkg.in/raintank/schema.v1"
)

func generateChunks(b *testing.B) []chunk.IterGen {
	res := make([]chunk.IterGen, 0, b.N)

	// the metric is initialized with the first chunk at ts 1,
	// so the generated slice should start at ts 2
	for i := uint32(2); i < uint32(b.N+2); i++ {
		c := getItgen(b, []uint32{1}, i, true)
		res = append(res, c)
	}
	return res
}

func initMetric(b *testing.B) *CCacheMetric {
	ccm := NewCCacheMetric()
	mkey, _ := schema.MKeyFromString("1.12345678901234567890123456789012")
	c := getItgen(b, []uint32{1}, uint32(1), true)
	ccm.Init(mkey, 0, c)
	return ccm
}

func BenchmarkAddingManyChunksOneByOne(b *testing.B) {
	ccm := initMetric(b)
	chunks := generateChunks(b)
	prev := uint32(1)
	b.ResetTimer()
	for _, chunk := range chunks {
		ccm.Add(prev, chunk)
		prev = chunk.Ts
	}
}

func BenchmarkAddingManyChunksAtOnce(b *testing.B) {
	ccm := initMetric(b)
	chunks := generateChunks(b)
	prev := uint32(1)
	b.ResetTimer()
	ccm.AddSlice(prev, chunks)
}
