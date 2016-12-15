package cache

import (
	"testing"
)

// some test data
var metric1 string = "metric1"
var metric2 string = "metric2"
var ts1 uint32 = 1
var ts2 uint32 = 2
var sizes []uint64 = []uint64{1, 2, 3, 4}

func addTestData(a *Accounting) bool {
	var res [4]bool

	res[0] = a.Add(metric1, ts1, sizes[0])
	res[1] = a.Add(metric1, ts2, sizes[1])
	res[2] = a.Add(metric2, ts1, sizes[2])
	res[3] = a.Add(metric2, ts2, sizes[3])

	if !(res[0] && res[1] && res[2] && res[3]) {
		return false
	}
	return true
}

func TestAdding(t *testing.T) {
	var expected uint64 = 0
	var val uint64
	var res bool

	a := NewAccounting()
	res = addTestData(a)
	if res != true {
		t.Fatalf("Expected result to be true")
	}

	for _, val = range sizes {
		expected = expected + val
	}

	total := a.GetTotal()
	if total != expected {
		t.Fatalf("Expected a total of %d, but got %d", expected, total)
	}
}

func TestDeleting(t *testing.T) {
	var res bool

	a := NewAccounting()
	res = addTestData(a)
	if res != true {
		t.Fatalf("Expected result to be true")
	}

	res = a.Del(metric1, ts2)
	if res != true {
		t.Fatalf("Expected result to be true")
	}

	var expected uint64 = sizes[0] + sizes[2] + sizes[3]

	total := a.GetTotal()
	if total != expected {
		t.Fatalf("Expected a total of %d, but got %d", expected, total)
	}
}

func TestDeletingInvalidValues(t *testing.T) {
	var res bool

	a := NewAccounting()
	res = addTestData(a)
	if res != true {
		t.Fatalf("Expected result to be true")
	}

	res = a.Del(metric1, ts2+1)
	if res != false {
		t.Fatalf("Expected result to be false")
	}

	res = a.Del("nonexistent", ts1)
	if res != false {
		t.Fatalf("Expected result to be false")
	}
}

func TestAddingInvalidValues(t *testing.T) {
	var res bool

	a := NewAccounting()
	res = addTestData(a)
	if res != true {
		t.Fatalf("Expected result to be true")
	}

	// already present
	res = a.Add(metric1, ts1, sizes[0])
	if res != false {
		t.Fatalf("Expected result to be false")
	}
}
