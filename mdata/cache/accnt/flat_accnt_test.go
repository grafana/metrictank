package accnt

import (
	"testing"

	"github.com/grafana/metrictank/test"

	"gopkg.in/raintank/schema.v1"
)

func resetCounters() {
	cacheChunkAdd.SetUint32(0)
	cacheChunkEvict.SetUint32(0)
	cacheSizeUsed.SetUint64(0)
	cacheCapUsed.SetUint64(0)
}

func TestAddingEvicting(t *testing.T) {
	resetCounters()
	a := NewFlatAccnt(10)
	evictQ := a.GetEvictQ()

	// some test data
	var et *EvictTarget
	metric1 := schema.GetAMKey(test.GetMKey(1), schema.Cnt, 600)
	metric2 := schema.GetAMKey(test.GetMKey(2), schema.Cnt, 600)
	var ts1 uint32 = 1
	var ts2 uint32 = 2

	a.AddChunk(metric1, ts1, 3, 3) // total size now 3
	a.AddChunk(metric1, ts2, 3, 3) // total size now 6
	a.AddChunk(metric2, ts1, 3, 3) // total size now 9
	a.AddChunk(metric2, ts2, 5, 5) // total size now 14

	et = <-evictQ // total size now 11
	if et.Metric != metric1 || et.Ts != ts1 {
		t.Fatalf("Returned evict target is not as expected, got %+v", et)
	}

	et = <-evictQ // total size now 8
	if et.Metric != metric1 || et.Ts != ts2 {
		t.Fatalf("Returned evict target is not as expected, got %+v", et)
	}

	select {
	case et := <-evictQ:
		t.Fatalf("Expected the EvictQ to be empty, got %+v", et)
	default:
	}

	// hitting metric2 ts1 to reverse order in LRU
	a.HitChunk(metric2, ts1)

	// total size now 18. evict everything else, because 10 is max size
	a.AddChunk(metric1, ts1, 10, 10)

	et = <-evictQ // total size now 15
	// Despite reversed order in LRU, the chronologically older ts should be first
	if et.Metric != metric2 || et.Ts != ts1 {
		t.Fatalf("Returned evict target is not as expected, got %+v", et)
	}

	et = <-evictQ // total size now 10
	// Next comes the original target ts
	if et.Metric != metric2 || et.Ts != ts2 {
		t.Fatalf("Returned evict target is not as expected, got %+v", et)
	}

	select {
	case et := <-evictQ:
		t.Fatalf("Expected the EvictQ to be empty, got %+v", et)
	default:
	}

	if peek := cacheChunkAdd.Peek(); peek != 5 {
		t.Fatalf("Expected add counter to be at 5, got %d", peek)
	}

	if peek := cacheChunkEvict.Peek(); peek != 4 {
		t.Fatalf("Expected evict counter to be at 4, got %d", peek)
	}

	a.Stop()
}

func TestLRUOrdering(t *testing.T) {
	resetCounters()
	a := NewFlatAccnt(6)
	evictQ := a.GetEvictQ()

	// some test data
	var et *EvictTarget
	metric1 := schema.GetAMKey(test.GetMKey(1), schema.Cnt, 600)
	metric2 := schema.GetAMKey(test.GetMKey(2), schema.Cnt, 600)
	metric3 := schema.GetAMKey(test.GetMKey(3), schema.Cnt, 600)
	var ts1 uint32 = 1

	a.AddChunk(metric1, ts1, 3, 3) // total size now 3
	a.AddChunk(metric2, ts1, 3, 3) // total size now 6

	// this should reverse the order in the LRU
	a.HitChunk(metric1, ts1)

	a.AddChunk(metric3, ts1, 3, 3) // total size now 9
	et = <-evictQ                  // total size now 6
	if et.Metric != metric2 || et.Ts != ts1 {
		t.Fatalf("Returned evict target is not as expected, got %+v", et)
	}

	if peek := cacheChunkAdd.Peek(); peek != 3 {
		t.Fatalf("Expected add counter to be at 3, got %d", peek)
	}

	if peek := cacheChunkEvict.Peek(); peek != 1 {
		t.Fatalf("Expected evict counter to be at 1, got %d", peek)
	}

	a.Stop()
}

func TestMetricDeleting(t *testing.T) {
	resetCounters()
	a := NewFlatAccnt(12)

	metric1 := schema.GetAMKey(test.GetMKey(1), schema.Cnt, 600)
	metric2 := schema.GetAMKey(test.GetMKey(2), schema.Cnt, 600)

	a.AddChunk(metric1, 1, 2, 3)
	a.AddChunk(metric2, 1, 2, 3)
	a.AddChunk(metric1, 2, 2, 3)
	a.AddChunk(metric2, 2, 2, 3)
	a.AddChunk(metric1, 3, 2, 3)
	a.AddChunk(metric2, 3, 2, 3)

	a.DelMetric(metric1)

	total := a.GetTotal()
	expect_total_cap := uint64(9)
	expect_total_size := uint64(6)
	if total != expect_total_size {
		t.Fatalf("Expected total %d, got %d", expect_total_size, total)
	}

	if cacheSizeUsed.Peek() != expect_total_size {
		t.Fatalf("Expected total %d, got %d", expect_total_size, total)
	}

	total_cache_cap := cacheCapUsed.Peek()
	if total_cache_cap != expect_total_cap {
		t.Fatalf("Expected total %d, got %d", expect_total_cap, total_cache_cap)
	}

	if _, ok := a.metrics[metric1]; ok {
		t.Fatalf("Expected %s to not exist, but it's still present", metric1)
	}

	a.AddChunk(metric1, 4, 12, 12)
	evictQ := a.GetEvictQ()

	et := <-evictQ
	if et.Metric != metric2 || et.Ts != 1 {
		t.Fatalf("Returned evict target is not as expected, got %+v", et)
	}
	et = <-evictQ
	if et.Metric != metric2 || et.Ts != 2 {
		t.Fatalf("Returned evict target is not as expected, got %+v", et)
	}
	et = <-evictQ
	if et.Metric != metric2 || et.Ts != 3 {
		t.Fatalf("Returned evict target is not as expected, got %+v", et)
	}

	a.Stop()
}
