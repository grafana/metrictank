package keycache

import (
	"sync"
	"time"

	"github.com/grafana/metrictank/schema"
)

// SubKey is the last 15 bytes of a 16 byte Key
// We can track Key-identified metrics with a SubKey because
// we shard by the first byte of the Key.
type SubKey [15]byte

// Shard tracks for each SubKey when it was last seen
// we know the last seen timestamp with ~10 minute precision
// because all SubKey's Duration's are relative to the ref
type Shard struct {
	sync.Mutex
	ref  Ref
	data map[SubKey]Duration
}

// NewShard creates a new shard
func NewShard(ref Ref) Shard {
	return Shard{
		ref:  ref,
		data: make(map[SubKey]Duration),
	}
}

// Touch marks the key as seen and returns whether it was seen before
// callers should assure that t >= ref and t-ref <= 42 hours
func (s *Shard) Touch(key schema.Key, t time.Time) bool {
	var sub SubKey
	copy(sub[:], key[1:])
	s.Lock()
	_, ok := s.data[sub]
	s.data[sub] = NewDuration(s.ref, t)
	s.Unlock()
	return ok
}

// Len returns the length of the shard
func (s *Shard) Len() int {
	s.Lock()
	l := len(s.data)
	s.Unlock()
	return l
}

// Prune removes stale entries from the shard.
// for this to work effectively,
// call this with a frequency > 10 min and < 42 hours
func (s *Shard) Prune(now time.Time, cutoff Duration) int {

	s.Lock()
	defer s.Unlock()

	// establish the new reference.
	// any values older will be pruned
	// any values newer or equal will have their duration adjusted relative to newRef
	newref := NewRef(now) - Ref(cutoff)

	// in this case, nothing we can do.
	// this happens when:
	// caller prunes too often (more frequently than every 10 minutes)
	// clock was adjusted and we went back in time
	// the cutoff has increased from one call to the next
	if newref <= s.ref {
		return len(s.data)
	}

	// if we went further ahead in time than our bookkeeping resolution
	// prune everything
	// this happens when:
	// clock jumps ahead
	// missed ticker reads
	// poorly configured prune interval
	if newref > s.ref+255 {
		s.data = make(map[SubKey]Duration)
		return 0
	}

	// now that we know for sure that s.ref < newref <= s.ref+255
	// or 0 < newref - s.ref <= 255                                                     (1)
	// we can do precise pruning

	// the amount to subtract of a duration for it to be based on the new reference
	subtract := Duration(newref - s.ref)

	for subkey, duration := range s.data {
		// remove entry if it is too old, e.g. if:
		// newref > "timestamp of the entry in 10minutely buckets"
		// newref > s.ref + duration                                                (2)
		if newref > s.ref+Ref(duration) {
			delete(s.data, subkey)
			continue
		}

		// adjust the duration to be based on the new reference
		// for this to be correct we must assert that there is no underflow,
		// ie. that: duration - subtract >= 0
		// because of (2) we know that:
		// newref <= s.ref+duration
		// iow duration >= newref - s.ref
		// iow duration >= subtract
		// iow duration - bustract >=0 (QED)
		s.data[subkey] = duration - subtract
	}
	s.ref = newref
	return len(s.data)
}
