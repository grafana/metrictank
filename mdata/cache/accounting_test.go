package cache

import (
	"testing"
)

func TestAdding(t *testing.T) {
	a := NewAccounting()

	var metric1 string = "metric1"
	var metric2 string = "metric2"
	var ts1 uint32 = 1
	var ts2 uint32 = 2
	sizes := []uint64{1, 2, 3, 4}

	a.Add(metric1, ts1, sizes[0])
	a.Add(metric1, ts2, sizes[1])
	a.Add(metric2, ts1, sizes[2])
	a.Add(metric2, ts2, sizes[3])

	var expected uint64 = 0
	var val uint64
	for _, val = range sizes {
		expected = expected + val
	}

	total := a.GetTotal()
	if total != expected {
		t.Fatalf("Expected a total of %d, but got %d", expected, total)
	}
}
