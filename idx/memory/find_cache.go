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
	// metric idx.memory.find-cache.ops.hit is a counter of findCache hits
	findCacheHit = stats.NewCounterRate32("idx.memory.find-cache.ops.hit")
	// metric idx.memory.find-cache.ops.miss is a counter of findCache misses
	findCacheMiss = stats.NewCounterRate32("idx.memory.find-cache.ops.miss")
	// metric idx.memory.find-cache.backoff tracks whether the find cache is in backoff mode
	findCacheBackoff = stats.NewBool("idx.memory.find-cache.backoff")
	// metric idx.memory.find-cache.invalidation.recv is the number of received invalidation requests
	findCacheInvalidationsReceived = stats.NewCounterRate32("idx.memory.find-cache.invalidation.recv")
	// metric idx.memory.find-cache.invalidation.exec is the number of executed invalidation requests
	findCacheInvalidationsExecuted = stats.NewCounterRate32("idx.memory.find-cache.invalidation.exec")
	// metric idx.memory.find-cache.invalidation.drop is the number of dropped invalidation requests
	findCacheInvalidationsDropped = stats.NewCounterRate32("idx.memory.find-cache.invalidation.drop")
)

type invalidateRequest struct {
	orgId uint32
	path  string
}

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
	size                int
	invalidateQueueSize int
	invalidateMaxSize   int
	invalidateMaxWait   time.Duration
	backoffTime         time.Duration

	shutdown              chan struct{}
	invalidateReqs        chan invalidateRequest
	forceInvalidationReq  chan struct{}
	forceInvalidationResp chan struct{}

	cache   map[uint32]*lru.Cache
	backoff time.Time
	sync.RWMutex
}

func NewFindCache(size, invalidateQueueSize, invalidateMaxSize int, invalidateMaxWait, backoffTime time.Duration) *FindCache {
	fc := &FindCache{
		size:                size,
		invalidateQueueSize: invalidateQueueSize,
		invalidateMaxSize:   invalidateMaxSize,
		invalidateMaxWait:   invalidateMaxWait,
		backoffTime:         backoffTime,

		shutdown:              make(chan struct{}),
		invalidateReqs:        make(chan invalidateRequest, invalidateQueueSize),
		forceInvalidationReq:  make(chan struct{}),
		forceInvalidationResp: make(chan struct{}),

		cache: make(map[uint32]*lru.Cache),
	}
	go fc.processInvalidateQueue()
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
	backoff := c.backoff
	c.RUnlock()
	var err error
	if !ok {
		// don't init the cache if we are in backoff mode.
		if time.Until(backoff) > 0 {
			return
		}
		c.Lock()
		// re-check. someone else may have added a cache in the meantime.
		cache, ok = c.cache[orgId]
		if !ok {
			cache, err = lru.New(c.size)
			if err != nil {
				log.Errorf("memory-idx: findCache failed to create lru. err=%s", err)
				c.Unlock()
				return
			}
			c.cache[orgId] = cache
		}
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
	c.Lock()
	findCacheInvalidationsReceived.Inc()
	defer c.Unlock()
	if time.Now().Before(c.backoff) {
		findCacheInvalidationsDropped.Inc()
		return
	}

	cache, ok := c.cache[orgId]
	if !ok || cache.Len() < 1 {
		findCacheInvalidationsDropped.Inc()
		return
	}

	req := invalidateRequest{
		orgId: orgId,
		path:  path,
	}
	select {
	case c.invalidateReqs <- req:
	default:
		c.triggerBackoff()
		return
	}
}

// caller must hold lock!
func (c *FindCache) triggerBackoff() {
	log.Infof("memory-idx: findCache invalidate-queue full. Disabling cache for %s", c.backoffTime.String())
	c.backoff = time.Now().Add(c.backoffTime)
	findCacheBackoff.SetTrue()
	time.AfterFunc(c.backoffTime, findCacheBackoff.SetFalse)
	c.cache = make(map[uint32]*lru.Cache)
	// drain queue
L:
	for {
		select {
		case <-c.invalidateReqs:
		default:
			break L
		}
	}
}

func (c *FindCache) forceInvalidation() {
	c.forceInvalidationReq <- struct{}{}
	<-c.forceInvalidationResp
}

func (c *FindCache) Shutdown() {
	c.shutdown <- struct{}{}
}

func (c *FindCache) processInvalidateQueue() {
	type invalidateBuffer struct {
		buffer map[uint32][]invalidateRequest
		count  uint32
	}
	timer := time.NewTimer(c.invalidateMaxWait)
	buf := invalidateBuffer{
		buffer: make(map[uint32][]invalidateRequest),
	}

	processQueue := func() {
		for orgid, reqs := range buf.buffer {
			c.RLock()
			cache := c.cache[orgid]
			c.RUnlock()

			// nothing cached for this org. nothing to do
			if cache == nil {
				findCacheInvalidationsDropped.Inc()
				continue
			}

			// construct a tree including all of the now-invalid paths
			// we can then call `find(tree, pattern)` for each pattern in the cache and purge it if it matches the tree
			// we can't simply prune all cache keys that equal path or a subtree of it, because
			// what's cached are search patterns which may contain wildcards and other expressions
			tree := newBareTree()
			for _, req := range reqs {
				tree.add(req.path)
			}

			for _, k := range cache.Keys() {
				matches, err := find((*Tree)(tree), k.(string))
				if err != nil {
					log.Errorf("memory-idx: checking if cache key %q matches any of the %d invalid patterns resulted in error: %s", k, len(reqs), err)
				}
				if err != nil || len(matches) > 0 {
					cache.Remove(k)
				}
			}
			findCacheInvalidationsExecuted.Add(len(reqs))
		}
		// all done, reset the buffer
		buf.buffer = make(map[uint32][]invalidateRequest)
		buf.count = 0
	}

	for {
		select {
		case <-timer.C:
			timer.Reset(c.invalidateMaxWait)
			if buf.count > 0 {
				processQueue()
			}
		case <-c.forceInvalidationReq:
			if !timer.Stop() {
				<-timer.C
			}
			timer.Reset(c.invalidateMaxWait)
			if buf.count > 0 {
				processQueue()
			}
			c.forceInvalidationResp <- struct{}{}

		case req := <-c.invalidateReqs:
			if len(c.invalidateReqs) == c.invalidateQueueSize-1 {
				log.Info("memory-idx: findCache invalidation channel was full, clearing findCache") // note: responsibility of InvalidateFor to set up backoff
				c.Lock()
				c.cache = make(map[uint32]*lru.Cache)
				c.Unlock()
				buf.buffer = make(map[uint32][]invalidateRequest)
				buf.count = 0
				continue
			}
			buf.buffer[req.orgId] = append(buf.buffer[req.orgId], req)
			buf.count++
			if int(buf.count) >= c.invalidateQueueSize {
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(c.invalidateMaxWait)
				processQueue()
			}
		case <-c.shutdown:
			return
		}
	}
}

// PurgeFindCache purges the findCaches for all orgIds
func (m *UnpartitionedMemoryIdx) PurgeFindCache() {
	m.findCache.PurgeAll()
}

// ForceInvalidationFindCache forces a full invalidation cycle of the find cache
func (m *UnpartitionedMemoryIdx) ForceInvalidationFindCache() {
	m.findCache.forceInvalidation()
}

// PurgeFindCache purges the findCaches for all orgIds
// across all partitions
func (p *PartitionedMemoryIdx) PurgeFindCache() {
	for _, m := range p.Partition {
		m.findCache.PurgeAll()
	}
}

// ForceInvalidationFindCache forces a full invalidation cycle of the find cache
func (p *PartitionedMemoryIdx) ForceInvalidationFindCache() {
	for _, m := range p.Partition {
		m.findCache.forceInvalidation()
	}
}

// BareTree is a Tree that can be used for finds,
// but is incomplete
// (it does not track actual MKey's or multiple Defs with same path)
// so should not be used as an actual index
type bareTree Tree

func newBareTree() *bareTree {
	tree := Tree{
		Items: map[string]*Node{
			"": {},
		},
	}
	return (*bareTree)(&tree)
}

// Add adds the series path,
// creating the nodes for each branch and the leaf node
// based on UnpartitionedMemoryIdx.add() but without all the stuff
// we don't need
func (tree *bareTree) add(path string) {
	// if a node already exists under the same path, nothing left to do
	if _, ok := tree.Items[path]; ok {
		return
	}

	pos := strings.LastIndex(path, ".")

	// now walk backwards through the node path to find the first branch which exists that
	// this path extends and add us as a child
	// until then, keep adding new intermediate branches
	prevPos := len(path)
	for pos != -1 {
		branch := path[:pos]
		prevNode := path[pos+1 : prevPos]
		if n, ok := tree.Items[branch]; ok {
			n.Children = append(n.Children, prevNode)
			break
		}

		tree.Items[branch] = &Node{
			Path:     branch,
			Children: []string{prevNode},
		}

		prevPos = pos
		pos = strings.LastIndex(branch, ".")
	}

	if pos == -1 {
		// no existing branches found that match. need to add to the root node.
		branch := path[:prevPos]
		n := tree.Items[""]
		n.Children = append(n.Children, branch)
	}

	// Add leaf node
	tree.Items[path] = &Node{
		Path: path,
		Defs: []schema.MKey{},
	}
}
