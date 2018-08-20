package cache

import (
	"testing"

	"github.com/grafana/metrictank/mdata/chunk"
	"github.com/grafana/metrictank/test"
	"github.com/raintank/schema"
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

func getCCM() (schema.AMKey, *CCacheMetric) {
	amkey, _ := schema.AMKeyFromString("1.12345678901234567890123456789012")
	ccm := NewCCacheMetric(amkey.MKey)
	return amkey, ccm
}

// TestAddAsc tests adding ascending timestamp chunks individually
func TestAddAsc(t *testing.T) {
	testRun(t, func(ccm *CCacheMetric) {
		chunks := generateChunks(t, 10, 6, 10)
		prev := uint32(1)
		for _, chunk := range chunks {
			ccm.Add(prev, chunk)
			prev = chunk.Ts
		}
	})
}

// TestAddDesc1 tests adding chunks that are all descending
func TestAddDesc1(t *testing.T) {
	testRun(t, func(ccm *CCacheMetric) {
		chunks := generateChunks(t, 10, 6, 10)
		for i := len(chunks) - 1; i >= 0; i-- {
			ccm.Add(0, chunks[i])
		}
	})
}

// TestAddDesc4 tests adding chunks that are globally descending
// but in groups 4 are ascending
func TestAddDesc4(t *testing.T) {
	testRun(t, func(ccm *CCacheMetric) {
		chunks := generateChunks(t, 10, 6, 10)
		ccm.Add(0, chunks[2])
		ccm.Add(0, chunks[3])
		ccm.Add(0, chunks[4])
		ccm.Add(0, chunks[5])
		ccm.Add(0, chunks[0])
		ccm.Add(0, chunks[1])
	})
}

// TestAddRange tests adding a contiguous range at once
func TestAddRange(t *testing.T) {
	testRun(t, func(ccm *CCacheMetric) {
		chunks := generateChunks(t, 10, 6, 10)
		prev := uint32(10)
		ccm.AddRange(prev, chunks)
	})
}

// TestAddRangeDesc4 benchmarks adding chunks that are globally descending
// but in groups of 4 are ascending. those groups are added via 1 AddRange.
func TestAddRangeDesc4(t *testing.T) {
	testRun(t, func(ccm *CCacheMetric) {
		chunks := generateChunks(t, 10, 6, 10)
		ccm.AddRange(0, chunks[2:6])
		ccm.AddRange(0, chunks[0:2])
	})
}

// test executes the run function
// run should generate chunks and add them to the CCacheMetric however it likes,
// but in a way so that the result will be as expected
func testRun(t *testing.T, run func(*CCacheMetric)) {
	amkey, ccm := getCCM()

	run(ccm)

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

// BenchmarkAddAsc benchmarks adding ascending timestamp chunks individually
func BenchmarkAddAsc(b *testing.B) {
	_, ccm := getCCM()
	chunks := generateChunks(b, 10, uint32(b.N), 10)
	prev := uint32(1)
	b.ResetTimer()
	for _, chunk := range chunks {
		ccm.Add(prev, chunk)
		prev = chunk.Ts
	}
}

// BenchmarkAddDesc1 benchmarks adding chunks that are all descending
func BenchmarkAddDesc1(b *testing.B) {
	_, ccm := getCCM()
	chunks := generateChunks(b, 10, uint32(b.N), 10)
	b.ResetTimer()
	for i := len(chunks) - 1; i >= 0; i-- {
		ccm.Add(0, chunks[i])
	}
}

// BenchmarkAddDesc4 benchmarks adding chunks that are globally descending
// but in groups 4 are ascending
func BenchmarkAddDesc4(b *testing.B) {
	_, ccm := getCCM()
	b.N = b.N - (b.N % 4)
	chunks := generateChunks(b, 10, uint32(b.N), 10)
	b.ResetTimer()
	for i := len(chunks) - 4; i >= 0; i -= 4 {
		ccm.Add(0, chunks[i])
		ccm.Add(0, chunks[i+1])
		ccm.Add(0, chunks[i+2])
		ccm.Add(0, chunks[i+3])
	}
}

// BenchmarkAddDesc64 benchmarks adding chunks that are globally descending
// but in groups 64 are ascending
func BenchmarkAddDesc64(b *testing.B) {
	_, ccm := getCCM()
	b.N = b.N - (b.N % 64)
	chunks := generateChunks(b, 10, uint32(b.N), 10)
	b.ResetTimer()
	for i := len(chunks) - 64; i >= 0; i -= 64 {
		for offset := 0; offset < 64; offset += 1 {
			ccm.Add(0, chunks[i+offset])
		}
	}

}

// BenchmarkAddRangeAsc benchmarks adding a contiguous range at once
func BenchmarkAddRangeAsc(b *testing.B) {
	_, ccm := getCCM()
	chunks := generateChunks(b, 10, uint32(b.N), 10)
	prev := uint32(1)
	b.ResetTimer()
	ccm.AddRange(prev, chunks)
}

// BenchmarkAddRangeDesc4 benchmarks adding chunks that are globally descending
// but in groups 4 are ascending. those groups are added via 1 AddRange.
func BenchmarkAddRangeDesc4(b *testing.B) {
	_, ccm := getCCM()
	b.N = b.N - (b.N % 4)
	chunks := generateChunks(b, 10, uint32(b.N), 10)
	b.ResetTimer()
	for i := len(chunks) - 4; i >= 0; i -= 4 {
		ccm.AddRange(0, chunks[i:i+4])
	}
}

// BenchmarkAddRangeDesc64 benchmarks adding chunks that are globally descending
// but in groups 64 are ascending. those groups are added via 1 AddRange.
func BenchmarkAddRangeDesc64(b *testing.B) {
	_, ccm := getCCM()
	b.N = b.N - (b.N % 64)
	chunks := generateChunks(b, 10, uint32(b.N), 10)
	b.ResetTimer()
	for i := len(chunks) - 64; i >= 0; i -= 64 {
		ccm.AddRange(0, chunks[i:i+64])
	}
}
