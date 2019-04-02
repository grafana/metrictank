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
	invalidateWaitTime  time.Duration
	shutdown            chan struct{}
	invalidateQueue     chan invalidateRequest

	cache     map[uint32]*lru.Cache
	newSeries map[uint32]chan struct{}
	backoff   map[uint32]time.Time
	sync.RWMutex
}

func NewFindCache(size, invalidateQueueSize int, invalidateWaitTime time.Duration) *FindCache {
	fc := &FindCache{
		cache:               make(map[uint32]*lru.Cache),
		size:                size,
		invalidateQueueSize: invalidateQueueSize,
		invalidateWaitTime:  invalidateWaitTime,
		newSeries:           make(map[uint32]chan struct{}),
		backoff:             make(map[uint32]time.Time),
		invalidateQueue:     make(chan invalidateRequest, invalidateQueueSize*2),
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
	if time.Now().Before(c.backoff[orgId]) {
		return
	}

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
		// TODO: set this back to a minute, move it back down to processQueue
		c.backoff[orgId] = time.Now().Add(c.invalidateWaitTime)
		delete(c.cache, orgId)
		c.Unlock()
		for i := 0; i < len(ch); i++ {
			select {
			case <-ch:
			default:
			}
		}
		log.Infof("memory-idx: findCache invalidate-queue full. Disabling cache for %s. num-cached-entries=%d", c.invalidateWaitTime.String(), cache.Len())
		return
	}
	c.invalidateQueue <- invalidateRequest{
		orgId: orgId,
		path:  path,
	}
}

func (c *FindCache) processInvalidateQueue() {
	type invalidateBuffer struct {
		bufffer map[uint32][]invalidateRequest
		count   uint32
	}
	timer := time.NewTimer(c.invalidateWaitTime)
	buf := invalidateBuffer{
		bufffer: make(map[uint32][]invalidateRequest),
	}
	var byOrg map[uint32][]invalidateRequest

	dumpCache := func() {
		c.PurgeAll()
	}

	processQueue := func() {
		for i, ir := range buf {
			byOrg = make(map[uint32][]invalidateRequest)
			byOrg[ir.orgId] = append(byOrg[ir.orgId], ir)
		}

		for _, irs := range byOrg {
			if len(irs) < 1 {
				continue
			}
			for i, ir := range irs {

			}
		}

		// convert our path to a tree so that we can call `find(tree, pattern)`
		// for each pattern in the cache and purge it if it matches the path or a subtree of it.
		// we can't simply prune all cache keys that equal path or a subtree of it, because
		// what's cached are search patterns which may contain wildcards and other expressions
		tree := newBareTree()
		tree.add(path)

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

	for {
		select {
		case <-timer.C:
			timer.Reset(c.invalidateWaitTime)
			if len(c.invalidateQueue) > 0 {
				processQueue()
			}
		case iqr := <-c.invalidateQueue:
			if len(c.invalidateQueue) == c.invalidateQueueSize*2-1 {
				log.Info("memory-idx: findCache was full, clearing entire findCache")
				dumpCache()
			}
			buf.bufffer = append(buf.bufffer, iqr)
			if len(buf.bufffer) >= c.invalidateQueueSize {
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(c.invalidateWaitTime)
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

// PurgeFindCache purges the findCaches for all orgIds
// across all partitions
func (p *PartitionedMemoryIdx) PurgeFindCache() {
	for _, m := range p.Partition {
		m.findCache.PurgeAll()
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
