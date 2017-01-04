package accnt

import (
	"testing"
)

func TestAddingEvicting(t *testing.T) {
	a := NewFlatAccnt(10)
	cacheChunkAdd.SetUint32(0)
	cacheChunkEvict.SetUint32(0)
	evictQ := a.GetEvictQ()

	// some test data
	var et *EvictTarget
	var metric1 string = "metric1"
	var metric2 string = "metric2"
	var ts1 uint32 = 1
	var ts2 uint32 = 2

	a.AddChunk(metric1, ts1, 3) // total size now 3
	a.AddChunk(metric1, ts2, 3) // total size now 6
	a.AddChunk(metric2, ts1, 3) // total size now 9
	a.AddChunk(metric2, ts2, 5) // total size now 14

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
	a.AddChunk(metric1, ts1, 10)

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
}

func TestLRUOrdering(t *testing.T) {
	a := NewFlatAccnt(6)
	cacheChunkAdd.SetUint32(0)
	cacheChunkEvict.SetUint32(0)
	evictQ := a.GetEvictQ()

	// some test data
	var et *EvictTarget
	var metric1 string = "metric1"
	var metric2 string = "metric2"
	var metric3 string = "metric3"
	var ts1 uint32 = 1

	a.AddChunk(metric1, ts1, 3) // total size now 3
	a.AddChunk(metric2, ts1, 3) // total size now 6

	// this should reverse the order in the LRU
	a.HitChunk(metric1, ts1)

	a.AddChunk(metric3, ts1, 3) // total size now 9
	et = <-evictQ               // total size now 6
	if et.Metric != metric2 || et.Ts != ts1 {
		t.Fatalf("Returned evict target is not as expected, got %+v", et)
	}

	if peek := cacheChunkAdd.Peek(); peek != 3 {
		t.Fatalf("Expected add counter to be at 3, got %d", peek)
	}

	if peek := cacheChunkEvict.Peek(); peek != 1 {
		t.Fatalf("Expected evict counter to be at 1, got %d", peek)
	}
}
