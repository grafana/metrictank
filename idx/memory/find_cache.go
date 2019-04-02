package memory

import (
	"strings"
	"sync"
	"time"

	"github.com/grafana/metrictank/stats"
	lru "github.com/hashicorp/golang-lru"
	"github.com/raintank/schema"
	log "github.com/sirupsen/logrus"
)

var (
	// metric idx.memory.find-cache.hit is a counter of findCache hits
	findCacheHit = stats.NewCounterRate32("idx.memory.find-cache.hit")
	// metric idx.memory.find-cache.miss is a counter of findCache misses
	findCacheMiss = stats.NewCounterRate32("idx.memory.find-cache.miss")
)

// FindCache is a caching layer for the in-memory index. The cache provides
// per org LRU caches of patterns and the resulting []*Nodes from searches
// on the index.  Users should call `InvalidateFor(orgId, path)` when new
// entries are added to, or removed from the index to invalidate any cached
// patterns that match the path.
// `invalidateQueueSize` sets the maximum number of invalidations for
// a specific orgId that can be running at any time. If this number is exceeded
// then the cache for that orgId will be immediately purged and disabled for
// `backoffTime`.  This mechanism protects the instance from excessive resource
// usage when a large number of new series are added at once.
type FindCache struct {
	sync.RWMutex
	cache               map[uint32]*lru.Cache
	size                int
	invalidateQueueSize int
	backoffTime         time.Duration
	newSeries           map[uint32]chan struct{}
	backoff             map[uint32]time.Time
}

func NewFindCache(size, invalidateQueueSize int, backoffTime time.Duration) *FindCache {
	fc := &FindCache{
		cache:               make(map[uint32]*lru.Cache),
		size:                size,
		invalidateQueueSize: invalidateQueueSize,
		backoffTime:         backoffTime,
		newSeries:           make(map[uint32]chan struct{}),
		backoff:             make(map[uint32]time.Time),
	}
	return fc
}

func (c *FindCache) Get(orgId uint32, pattern string) ([]*Node, bool) {
	c.RLock()
	cache, ok := c.cache[orgId]
	c.RUnlock()
	if !ok {
		findCacheMiss.Inc()
		return nil, ok
	}
	nodes, ok := cache.Get(pattern)
	if !ok {
		findCacheMiss.Inc()
		return nil, ok
	}
	findCacheHit.Inc()
	return nodes.([]*Node), ok
}

func (c *FindCache) Add(orgId uint32, pattern string, nodes []*Node) {
	c.RLock()
	cache, ok := c.cache[orgId]
	t := c.backoff[orgId]
	c.RUnlock()
	var err error
	if !ok {
		// don't init the cache if we are in backoff mode.
		if time.Until(t) > 0 {
			return
		}
		cache, err = lru.New(c.size)
		if err != nil {
			log.Errorf("memory-idx: findCache failed to create lru. err=%s", err)
			return
		}
		c.Lock()
		c.cache[orgId] = cache
		c.newSeries[orgId] = make(chan struct{}, c.invalidateQueueSize)
		c.Unlock()
	}
	cache.Add(pattern, nodes)
}

// Purge clears the cache for the specified orgId
func (c *FindCache) Purge(orgId uint32) {
	c.RLock()
	cache, ok := c.cache[orgId]
	c.RUnlock()
	if !ok {
		return
	}
	cache.Purge()
}

// PurgeAll clears the caches for all orgIds
func (c *FindCache) PurgeAll() {
	c.RLock()
	orgs := make([]uint32, len(c.cache))
	i := 0
	for k := range c.cache {
		orgs[i] = k
		i++
	}
	c.RUnlock()
	for _, org := range orgs {
		c.Purge(org)
	}
}

// InvalidateFor removes entries from the cache for 'orgId'
// that match the provided path. If lots of InvalidateFor calls
// are made at once and we end up with `invalidateQueueSize` concurrent
// goroutines processing the invalidations, we purge the cache and
// disable it for `backoffTime`. Future InvalidateFor calls made during
// the backoff time will then return immediately.
func (c *FindCache) InvalidateFor(orgId uint32, path string) {
	c.RLock()
	ch := c.newSeries[orgId]
	cache, ok := c.cache[orgId]
	c.RUnlock()
	if !ok || cache.Len() < 1 {
		return
	}

	select {
	case ch <- struct{}{}:
	default:
		c.Lock()
		c.backoff[orgId] = time.Now().Add(c.backoffTime)
		delete(c.cache, orgId)
		c.Unlock()
		for i := 0; i < len(ch); i++ {
			select {
			case <-ch:
			default:
			}
		}
		log.Infof("memory-idx: findCache invalidate-queue full. Disabling cache for %s. num-cached-entries=%d", c.backoffTime.String(), cache.Len())
		return
	}

	// convert our path to a tree so that we can call `find(tree, pattern)`
	// for each pattern in the cache and purge it if it matches the path or a subtree of it.
	// we can't simply prune all cache keys that equal path or a subtree of it, because
	// what's cached are search patterns which may contain wildcards and other expressions
	tree := treeFromPath(path)

	for _, k := range cache.Keys() {
		matches, err := find(tree, k.(string))
		if err != nil {
			log.Errorf("memory-idx: checking if new series matches expressions in findCache. series=%s expr=%s err=%s", path, k, err)
			continue
		}
		if len(matches) > 0 {
			cache.Remove(k)
		}
	}
	select {
	case <-ch:
	default:
	}
}

// PurgeFindCache purges the findCaches for all orgIds
func (m *UnpartitionedMemoryIdx) PurgeFindCache() {
	m.findCache.PurgeAll()
}

// PurgeFindCache purges the findCaches for all orgIds
// across all partitions
func (p *PartitionedMemoryIdx) PurgeFindCache() {
	for _, m := range p.Partition {
		m.findCache.PurgeAll()
	}
}

// treeFromPath creates an index tree from a series path.
// The tree will have a single leaf node and nodes for
// each branch.
func treeFromPath(path string) *Tree {
	tree := Tree{
		Items: map[string]*Node{
			"": {},
		},
	}
	pos := strings.Index(path, ".")
	var parentBranch string
	prevPos := -1
	for {
		branch := path[:pos]
		thisNode := branch[prevPos+1:]

		tree.Items[parentBranch].Children = []string{thisNode}

		// create this branch/leaf
		tree.Items[branch] = &Node{
			Path: branch,
		}
		if branch == path {
			tree.Items[branch].Defs = []schema.MKey{{}}
			break
		}
		prevPos = pos
		nextPos := strings.Index(path[pos+1:], ".")
		if nextPos < 0 {
			pos = len(path)
		} else {
			pos = pos + nextPos + 1
		}
		parentBranch = branch
	}

	return &tree
}
