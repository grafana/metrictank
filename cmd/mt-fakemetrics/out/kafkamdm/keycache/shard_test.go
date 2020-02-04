package keycache

import (
	"testing"
	"time"

	"github.com/grafana/metrictank/schema"
)

func GetKey(suffix int) schema.Key {
	s := uint32(suffix)
	return schema.Key{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, byte(s >> 24), byte(s >> 16), byte(s >> 8), byte(s)}
}

func TestPrune(t *testing.T) {
	base := 1234567890
	now := time.Unix(int64(base), 0)

	shard := NewShard(NewRef(now))
	maxOffset := 255 * 600

	// add a bunch of keys, with a key named after their timestamp for convenience
	// starting at the base timestamp, all the way to base+(255*600)-1, they are bucketed as follows:
	// ref     range                 count
	// 2057613 base       - 1234568399 510
	// 2057614 1234568400 - 1234569000 600
	// ....
	// 2057867 1234720200 - 1234720799 600
	// 2057868 1234720800 - 1234720889 90
	for offset := 0; offset < maxOffset; offset++ {
		ok := shard.Touch(GetKey(offset), time.Unix(int64(base+offset), 0))
		if ok {
			t.Fatalf("offset=%d, Touch ok expected false, got true", offset)
		}
		l := shard.Len()
		if l != offset+1 {
			t.Fatalf("offset=%d, expected len %d, got %d", offset, offset+1, l)
		}
	}
	// assert behavior when we prune very quickly after last prune or after shard creation
	l := shard.Prune(now, 0)
	exp := maxOffset
	if l != exp {
		t.Fatalf("after prune, expected len %d, got %d", exp, l)
	}
	// also try a few different cutoffs
	l = shard.Prune(now, 1)
	if l != exp {
		t.Fatalf("after prune, expected len %d, got %d", exp, l)
	}
	l = shard.Prune(now, 2)
	if l != exp {
		t.Fatalf("after prune, expected len %d, got %d", exp, l)
	}
	l = shard.Prune(now, 3)
	if l != exp {
		t.Fatalf("after prune, expected len %d, got %d", exp, l)
	}

	// now start progressing through time and assert that stale entries get pruned
	l = shard.Prune(now.Add(10*time.Minute), 1)
	if l != exp {
		t.Fatalf("after prune, expected len %d, got %d", exp, l)
	}
	l = shard.Prune(now.Add(20*time.Minute), 1)
	exp = maxOffset - 510
	if l != exp {
		t.Fatalf("after prune, expected len %d, got %d", exp, l)
	}
	l = shard.Prune(now.Add(30*time.Minute), 1)
	exp -= 600
	if l != exp {
		t.Fatalf("after prune, expected len %d, got %d", exp, l)
	}

	// re-prune with same settings and also with more cutoff allowance, should remain where we are
	l = shard.Prune(now.Add(30*time.Minute), 1)
	if l != exp {
		t.Fatalf("after prune, expected len %d, got %d", exp, l)
	}
	l = shard.Prune(now.Add(30*time.Minute), 2)
	if l != exp {
		t.Fatalf("after prune, expected len %d, got %d", exp, l)
	}

	// continue again
	l = shard.Prune(now.Add(40*time.Minute), 1)
	exp -= 600
	if l != exp {
		t.Fatalf("after prune, expected len %d, got %d", exp, l)
	}

	// jump forward more significantly
	l = shard.Prune(now.Add(70*time.Minute), 1)
	exp -= 3 * 600
	if l != exp {
		t.Fatalf("after prune, expected len %d, got %d", exp, l)
	}

	// jump forward more significantly but with more cutoff allowance
	l = shard.Prune(now.Add(100*time.Minute), 3)
	exp -= 600
	if l != exp {
		t.Fatalf("after prune, expected len %d, got %d", exp, l)
	}
}

// TestPruneAdjustRef creates a shard with arbitrary ref, adds a point to it with arbitrary timestamp
// prunes it in the future, and asserts that the ref of the entry has been updated for the new reference
func TestPruneAdjustRef(t *testing.T) {
	base := 1234567890 // ref 2057613
	now := time.Unix(int64(base), 0)
	pointTs := 2057687*600 + 1337
	key := GetKey(42)

	shard := NewShard(NewRef(now))
	shard.Touch(key, time.Unix(int64(pointTs), 0))
	newNow := time.Unix(2057689*600, 10)
	l := shard.Prune(newNow, 2)
	exp := 1
	if l != exp {
		t.Fatalf("after prune, expected len %d, got %d", exp, l)
	}

	expDuration := Duration((pointTs / 600) - (2057689 - 2))
	var sub SubKey
	copy(sub[:], key[1:])
	if shard.data[sub] != expDuration {
		t.Fatalf("after prune, expected duration to have been adjusted to %d, but got %d", expDuration, shard.data[sub])
	}
}
