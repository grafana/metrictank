package accnt

import (
	"testing"
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
