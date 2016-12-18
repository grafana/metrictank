package cache

import (
	"testing"
)

func TestAdding(t *testing.T) {
	a := &FlatAccnt{
		total:   0,
		metrics: make(map[string]*FlatAccntMet),
		maxSize: 10,
		lru:     NewLRU(),
		evictQ:  make(chan *EvictTarget),
	}
	a.init()
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
	if et.Metric != metric2 || et.Ts != ts2 {
		t.Fatalf("Returned evict target is not as expected, got %+v", et)
	}
	et = <-evictQ
	if et.Metric != metric2 || et.Ts != ts1 {
		t.Fatalf("Returned evict target is not as expected, got %+v", et)
	}

	select {
	case et := <-evictQ:
		t.Fatalf("Expected the EvictQ to be empty, got %+v", et)
	default:
	}
}
