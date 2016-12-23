package accnt

import (
	"testing"
	"time"
)

func getNew(maxSize int) *FlatAccnt {
	a := &FlatAccnt{
		total:       0,
		metrics:     make(map[string]*FlatAccntMet),
		maxSize:     uint64(maxSize),
		lru:         NewLRU(),
		evictQ:      make(chan *EvictTarget, 10),
		eventQ:      make(chan *FlatAccntEvent, 10),
		stats:       &Stats{0, 0, 0, 0, 0, 0, 0, 0},
		lastPrint:   time.Now().UnixNano(),
		statsTicker: time.NewTicker(time.Second * 10),
	}
	a.statsTicker.Stop()
	go a.eventLoop()
	return a
}

func TestAddingEvicting(t *testing.T) {
	a := getNew(10)
	evictQ := a.GetEvictQ()

	// some test data
	var et *EvictTarget
	var metric1 string = "metric1"
	var metric2 string = "metric2"
	var ts1 uint32 = 1
	var ts2 uint32 = 2

	a.AddChunk(metric1, ts1, 3)
	a.AddChunk(metric1, ts2, 3)
	a.AddChunk(metric2, ts1, 3)
	a.AddChunk(metric2, ts2, 5)

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
	a.HitChunk(metric2, ts1)

	// evict everything else, because 10 is max size
	a.AddChunk(metric1, ts1, 10)

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

	if a.stats.add_chnk != 5 {
		t.Fatalf("Expected add counter to be at 5, got %d", a.stats.add_chnk)
	}

	if a.stats.hit_chnk != 1 {
		t.Fatalf("Expected hit counter to be at 1, got %d", a.stats.hit_chnk)
	}

	if a.stats.evict_chnk != 4 {
		t.Fatalf("Expected evict counter to be at 4, got %d", a.stats.evict_chnk)
	}
}

func TestLRUOrdering(t *testing.T) {
	a := getNew(6)
	evictQ := a.GetEvictQ()

	// some test data
	var et *EvictTarget
	var metric1 string = "metric1"
	var metric2 string = "metric2"
	var metric3 string = "metric3"
	var ts1 uint32 = 1

	a.AddChunk(metric1, ts1, 3)
	a.AddChunk(metric2, ts1, 3)

	// this should reverse the order in the LRU
	a.HitChunk(metric1, ts1)

	a.AddChunk(metric3, ts1, 3)
	et = <-evictQ
	if et.Metric != metric2 || et.Ts != ts1 {
		t.Fatalf("Returned evict target is not as expected, got %+v", et)
	}

	if a.stats.add_chnk != 3 {
		t.Fatalf("Expected add counter to be at 3, got %d", a.stats.add_chnk)
	}

	if a.stats.hit_chnk != 1 {
		t.Fatalf("Expected hit counter to be at 1, got %d", a.stats.hit_chnk)
	}

	if a.stats.evict_chnk != 1 {
		t.Fatalf("Expected evict counter to be at 1, got %d", a.stats.evict_chnk)
	}
}
