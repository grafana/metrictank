package cache

import (
	"errors"
	"fmt"
	"math/rand"
	"reflect"
	"sort"
	"testing"
	"time"

	"github.com/grafana/metrictank/mdata/cache/accnt"
	"github.com/grafana/metrictank/mdata/chunk"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/test"
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
			prev = chunk.T0
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
	if res.Type != Hit {
		t.Fatalf("expected ResultType Hit, got %v", res.Type)
	}

	if res.Start[0].T0 != 20 {
		t.Fatalf("Expected result to start at 20, but had %d", res.Start[0].T0)
	}

	if res.Start[len(res.Start)-1].T0 != 40 {
		t.Fatalf("Expected result to start at 40, but had %d", res.Start[len(res.Start)-1].T0)
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
		prev = chunk.T0
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

func TestCorruptionCase1(t *testing.T) {
	testRun(t, func(ccm *CCacheMetric) {
		chunks := generateChunks(t, 10, 6, 10)
		ccm.AddRange(0, chunks[3:6])
		ccm.AddRange(0, chunks[0:4])
		if err := verifyCcm(ccm, []uint32{10, 20, 30, 40, 50, 60}); err != nil {
			t.Fatal(err)
		}
	})
}

func getRandomNumber(min, max int) int {
	return rand.Intn(max-min) + min
}

// getRandomRange returns a range start-end so that
// end >= start and both numbers drawn from [min, max)
func getRandomRange(min, max int) (int, int) {
	number1 := getRandomNumber(min, max)
	number2 := getRandomNumber(min, max)
	if number1 > number2 {
		return number2, number1
	} else {
		return number1, number2
	}
}

func TestCorruptionCase2(t *testing.T) {
	rand.Seed(time.Now().Unix())
	_, ccm := getCCM()
	iterations := 100000
	var cached [100]bool // tracks which chunks should be cached
	var expKeys []uint32 // tracks which keys should be in the CCM

	// 100 chunks, first t0=10, last is t0=1000
	chunks := generateChunks(t, 10, 100, 10)

	var opAdd, opAddRange, opDel, opDelRange int
	var adds, dels int

	for i := 0; i < iterations; i++ {
		// 0 = Add
		// 1 = AddRange
		// 2 = Del
		// 3 = Del range (via multi del cals)
		action := getRandomNumber(0, 4)
		switch action {
		case 0:
			chunk := getRandomNumber(0, 100)
			//t.Logf("adding chunk %d", chunk)
			ccm.Add(0, chunks[chunk])
			cached[chunk] = true
			opAdd++
			adds++
		case 1:
			from, to := getRandomRange(0, 100)
			//t.Logf("adding range %d-%d", from, to)
			ccm.AddRange(0, chunks[from:to])
			for chunk := from; chunk < to; chunk++ {
				cached[chunk] = true
			}
			adds += (to - from)
			opAddRange++
		case 2:
			chunk := getRandomNumber(0, 100)
			//t.Logf("deleting chunk %d", chunk)
			ccm.Del(chunks[chunk].T0) // note: chunk may not exist
			cached[chunk] = false
			opDel++
			dels++
		case 3:
			from, to := getRandomRange(0, 100)
			//t.Logf("deleting range %d-%d", from, to)
			for chunk := from; chunk < to; chunk++ {
				ccm.Del(chunks[chunk].T0) // note: chunk may not exist
				cached[chunk] = false
			}
			opDelRange++
			dels += (to - from)
		}

		expKeys = expKeys[:0]
		for i, c := range cached {
			if c {
				expKeys = append(expKeys, uint32((i+1)*10))
			}
		}

		if err := verifyCcm(ccm, expKeys); err != nil {
			t.Fatal(err)
		}
	}

	fmt.Printf("operations: add %d - addRange %d - del %d - delRange %d\n", opAdd, opAddRange, opDel, opDelRange)
	fmt.Printf("total chunk adds %d - total chunk deletes %d\n", adds, dels)
}

// verifyCcm verifies the integrity of a CCacheMetric
// it assumes that all itergens are span-aware
func verifyCcm(ccm *CCacheMetric, expKeys []uint32) error {
	var chunk *CCacheChunk
	var ok bool

	if len(ccm.chunks) != len(ccm.keys) {
		return errors.New("Length of ccm.chunks does not match ccm.keys")
	}

	if !sort.IsSorted(accnt.Uint32Asc(ccm.keys)) {
		return errors.New("keys are not sorted")
	}

	if !reflect.DeepEqual(ccm.keys, expKeys) {
		return fmt.Errorf("keys mismatch. expected %v, got %v", expKeys, ccm.keys)
	}

	for i, ts := range ccm.keys {
		if chunk, ok = ccm.chunks[ts]; !ok {
			return fmt.Errorf("Ts %d is in ccm.keys but not in ccm.chunks", ts)
		}

		if i == 0 {
			if chunk.Prev != 0 {
				return errors.New("First chunk has Prev != 0")
			}
		} else {
			if chunk.Prev == 0 {
				if ccm.chunks[ccm.keys[i-1]].Ts == chunk.Ts-chunk.Itgen.Span() {
					return fmt.Errorf("Chunk of ts %d has Prev == 0, but the previous chunk is present", ts)
				}
			} else {
				if ccm.chunks[ccm.keys[i-1]].Ts != chunk.Prev {
					return fmt.Errorf("Chunk of ts %d has Prev set to wrong ts %d but should be %d", ts, chunk.Prev, ccm.chunks[ccm.keys[i-1]].Ts)
				}
			}
		}

		if i == len(ccm.keys)-1 {
			if chunk.Next != 0 {
				return fmt.Errorf("Next of last chunk should be 0, but it's %d", chunk.Next)
			}

			// all checks completed
			break
		}

		var nextChunk *CCacheChunk
		if nextChunk, ok = ccm.chunks[ccm.keys[i+1]]; !ok {
			return fmt.Errorf("Ts %d is in ccm.keys but not in ccm.chunks", ccm.keys[i+1])
		}

		if chunk.Next == 0 {
			if chunk.Ts+chunk.Itgen.Span() == nextChunk.Ts {
				return fmt.Errorf("Next of chunk at ts %d is set to 0, but the next chunk is present", ts)
			}
		} else {
			if chunk.Next != nextChunk.Ts {
				return fmt.Errorf("Next of chunk at ts %d is set to %d, but it should be %d", ts, chunk.Next, nextChunk.Ts)
			}
		}
	}
	return nil
}
