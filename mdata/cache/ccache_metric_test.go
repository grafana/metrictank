package cache

import (
	"testing"

	"github.com/grafana/metrictank/mdata/chunk"
	"github.com/grafana/metrictank/test"
	"gopkg.in/raintank/schema.v1"
)

func generateChunks(b testing.TB, startAt, count, step int) []chunk.IterGen {
	res := make([]chunk.IterGen, 0, count)

	values := make([]uint32, step)
	// the metric is initialized with the first chunk at ts 1,
	// so the generated slice should start at ts 2
	for i := startAt; i < (step*count)+startAt; i += step {
		c := getItgen(b, values, uint32(i), true)
		res = append(res, c)
	}
	return res
}

func initMetric(b testing.TB) (schema.MKey, *CCacheMetric) {
	ccm := NewCCacheMetric()
	mkey, _ := schema.MKeyFromString("1.12345678901234567890123456789012")
	c := getItgen(b, []uint32{1}, uint32(1), true)
	ccm.Init(mkey, 0, c)
	return mkey, ccm
}

func BenchmarkAddingManyChunksOneByOne(b *testing.B) {
	_, ccm := initMetric(b)
	chunks := generateChunks(b, 2, b.N, 1)
	prev := uint32(1)
	b.ResetTimer()
	for _, chunk := range chunks {
		ccm.Add(prev, chunk)
		prev = chunk.Ts
	}
}

func BenchmarkAddingManyChunksAtOnce(b *testing.B) {
	_, ccm := initMetric(b)
	chunks := generateChunks(b, 2, b.N, 1)
	prev := uint32(1)
	b.ResetTimer()
	ccm.AddSlice(prev, chunks)
}

func TestAddingChunksOneByOneAndQueryingThem(t *testing.T) {
	mkey, ccm := initMetric(t)
	amkey := schema.AMKey{MKey: mkey, Archive: 0}
	chunks := generateChunks(t, 10, 100, 10)
	prev := uint32(10)
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
	mkey, ccm := initMetric(t)
	amkey := schema.AMKey{MKey: mkey, Archive: 0}
	chunks := generateChunks(t, 10, 100, 10)
	prev := uint32(10)
	ccm.AddSlice(prev, chunks)

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
