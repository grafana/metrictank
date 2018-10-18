package accnt

import (
	"testing"

	"github.com/grafana/metrictank/test"
)

func TestLRU(t *testing.T) {
	key1 := "key1"
	key2 := "key2"

	lru := NewLRU()
	lru.touch(key1)
	lru.touch(key2)
	lru.touch(key1)
	lru.touch(key2)

	val := lru.pop()
	if val != key1 {
		t.Fatalf("expected %s, got %s", key1, val)
	}

	val = lru.pop()
	if val != key2 {
		t.Fatalf("expected %s, got %s", key2, val)
	}

	val = lru.pop()
	if val != nil {
		t.Fatalf("expected nil, got %s", val)
	}

	lru.touch(key1)
	lru.touch(key2)
	lru.touch(key1)
	val = lru.pop()
	if val != key2 {
		t.Fatalf("expected %s, got %s", key2, val)
	}
}

func TestLRUNumeric(t *testing.T) {
	key1 := 1000
	key2 := 1001

	lru := NewLRU()
	lru.touch(key1)
	lru.touch(key1)
	lru.touch(key1)
	lru.touch(key2)

	val := lru.pop()
	if val != key1 {
		t.Fatalf("expected %d, got %d", key1, val)
	}

	val = lru.pop()
	if val != key2 {
		t.Fatalf("expected %d, got %d", key2, val)
	}

	val = lru.pop()
	if val != nil {
		t.Fatalf("expected nil, got %d", val)
	}
}

func TestLRUDelete(t *testing.T) {
	key1 := 1000
	key2 := 1001

	lru := NewLRU()
	lru.touch(key1)
	lru.touch(key2)

	expectedSize := 2
	if len(lru.items) != expectedSize || lru.list.Len() != expectedSize {
		t.Fatalf("Expected lru to contain %d items, but have %d / %d", expectedSize, len(lru.items), lru.list.Len())
	}

	lru.del(key1)
	expectedSize = 1
	if len(lru.items) != expectedSize || lru.list.Len() != expectedSize {
		t.Fatalf("Expected lru to contain %d items, but have %d / %d", expectedSize, len(lru.items), lru.list.Len())
	}

	lru.del(key2)
	expectedSize = 0
	if len(lru.items) != expectedSize || lru.list.Len() != expectedSize {
		t.Fatalf("Expected lru to contain %d items, but have %d / %d", expectedSize, len(lru.items), lru.list.Len())
	}
}

// BenchmarkLRUGrowth is used to create comparisons of memory allocations as the LRU is optimized
func BenchmarkLRUGrowth(b *testing.B) {
	b.StopTimer()

	lru := NewLRU()

	var targets []EvictTarget

	amkey := test.GetAMKey(1)

	// create our EvictTargets
	for i := 0; i < (100 * 1024); i++ {
		targets = append(targets, EvictTarget{
			Metric: amkey,
			Ts:     uint32(i)})
	}

	b.StartTimer()
	for i := 0; i < b.N; i++ {
		for _, target := range targets {
			lru.touch(target)
		}
	}

}
