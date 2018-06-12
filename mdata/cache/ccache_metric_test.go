package cache

import (
	"testing"

	"github.com/grafana/metrictank/mdata/chunk"
	"github.com/grafana/metrictank/test"
	"gopkg.in/raintank/schema.v1"
)

func generateChunks(b testing.TB, startAt, count, step uint32) []chunk.IterGen {
	res := make([]chunk.IterGen, 0, count)

	values := make([]uint32, step)
	for t0 := startAt; t0 < startAt+(step*uint32(count)); t0 += step {
		c := getItgen(b, values, t0, true)
		res = append(res, c)
	}
	return res
}

func getCCM() (schema.MKey, *CCacheMetric) {
	mkey, _ := schema.MKeyFromString("1.12345678901234567890123456789012")
	ccm := NewCCacheMetric(mkey)
	return mkey, ccm
}

func BenchmarkAddingManyChunksOneByOne(b *testing.B) {
	_, ccm := getCCM()
	chunks := generateChunks(b, 10, uint32(b.N), 10)
	prev := uint32(1)
	b.ResetTimer()
	for _, chunk := range chunks {
		ccm.Add(prev, chunk)
		prev = chunk.Ts
	}
}

func BenchmarkAddingManyChunksAtOnce(b *testing.B) {
	_, ccm := getCCM()
	chunks := generateChunks(b, 10, uint32(b.N), 10)
	prev := uint32(1)
	b.ResetTimer()
	ccm.AddRange(prev, chunks)
}

func TestAddingChunksOneByOneAndQueryingThem(t *testing.T) {
	mkey, ccm := getCCM()
	amkey := schema.AMKey{MKey: mkey, Archive: 0}
	chunks := generateChunks(t, 10, 6, 10)
	prev := uint32(1)
	for _, chunk := range chunks {
		ccm.Add(prev, chunk)
		prev = chunk.Ts
	}

	res := CCSearchResult{}
	ccm.Search(test.NewContext(), amkey, &res, 25, 45)
	if res.Complete != true {
		t.Fatalf("Expected result to be complete, but it was not")
	}

	if res.Start[0].Ts != 20 {
		t.Fatalf("Expected result to start at 20, but had %d", res.Start[0].Ts)
	}

	if res.Start[len(res.Start)-1].Ts != 40 {
		t.Fatalf("Expected result to start at 40, but had %d", res.Start[len(res.Start)-1].Ts)
	}
}
func TestAddingChunksAtOnceAndQueryingThem(t *testing.T) {
	mkey, ccm := getCCM()
	amkey := schema.AMKey{MKey: mkey, Archive: 0}
	chunks := generateChunks(t, 10, 6, 10)
	prev := uint32(10)
	ccm.AddRange(prev, chunks)

	res := CCSearchResult{}
	ccm.Search(test.NewContext(), amkey, &res, 25, 45)
	if res.Complete != true {
		t.Fatalf("Expected result to be complete, but it was not")
	}

	if res.Start[0].Ts != 20 {
		t.Fatalf("Expected result to start at 20, but had %d", res.Start[0].Ts)
	}

	if res.Start[len(res.Start)-1].Ts != 40 {
		t.Fatalf("Expected result to start at 40, but had %d", res.Start[len(res.Start)-1].Ts)
	}
}
