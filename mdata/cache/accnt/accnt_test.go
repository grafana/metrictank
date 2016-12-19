package accnt

import (
	"testing"
)

func TestAddingEvicting(t *testing.T) {
	a := NewFlatAccnt(10)
	evictQ := a.GetEvictQ()

	// some test data
	var et *EvictTarget
	var metric1 string = "metric1"
	var metric2 string = "metric2"
	var ts1 uint32 = 1
	var ts2 uint32 = 2

	a.Add(metric1, ts1, 3)
	a.Add(metric1, ts2, 3)
	a.Add(metric2, ts1, 3)
	a.Add(metric2, ts2, 5)

	et = <-evictQ
	if et.Metric != metric1 || et.Ts != ts1 {
		t.Fatalf("Returned evict target is not as expected, got %+v", et)
	}
	et = <-evictQ
	if et.Metric != metric1 || et.Ts != ts2 {
		t.Fatalf("Returned evict target is not as expected, got %+v", et)
	}

	select {
	case et := <-evictQ:
		t.Fatalf("Expected the EvictQ to be empty, got %+v", et)
	default:
	}

	// hitting metric2 ts1 to reverse order in LRU
	a.Hit(metric2, ts1)

	// evict everything else, because 10 is max size
	a.Add(metric1, ts1, 10)

	et = <-evictQ
	// Despite reversed order in LRU, the chronologically older ts should be first
	if et.Metric != metric2 || et.Ts != ts1 {
		t.Fatalf("Returned evict target is not as expected, got %+v", et)
	}
	et = <-evictQ
	// Next comes the original target ts
	if et.Metric != metric2 || et.Ts != ts2 {
		t.Fatalf("Returned evict target is not as expected, got %+v", et)
	}

	select {
	case et := <-evictQ:
		t.Fatalf("Expected the EvictQ to be empty, got %+v", et)
	default:
	}
}

func TestLRUOrdering(t *testing.T) {
	a := NewFlatAccnt(6)
	evictQ := a.GetEvictQ()

	// some test data
	var et *EvictTarget
	var metric1 string = "metric1"
	var metric2 string = "metric2"
	var metric3 string = "metric3"
	var ts1 uint32 = 1

	a.Add(metric1, ts1, 3)
	a.Add(metric2, ts1, 3)

	// this should reverse the order in the LRU
	a.Hit(metric1, ts1)

	a.Add(metric3, ts1, 3)
	et = <-evictQ
	if et.Metric != metric2 || et.Ts != ts1 {
		t.Fatalf("Returned evict target is not as expected, got %+v", et)
	}
}
