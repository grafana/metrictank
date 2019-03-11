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
	// metric idx.memory.find-cache.hit is a counter of findCache misses
	findCacheMiss = stats.NewCounterRate32("idx.memory.find-cache.miss")

	findCacheSize            = 1000
	findCacheInvalidateQueue = 100
	findCacheBackoff         = time.Minute
)

type FindCache struct {
	cache map[uint32]*lru.Cache
	size  int
	sync.RWMutex
	newSeries map[uint32]chan struct{}
	backoff   map[uint32]time.Time
}

func NewFindCache(size int) *FindCache {
	fc := &FindCache{
		cache:     make(map[uint32]*lru.Cache),
		size:      size,
		newSeries: make(map[uint32]chan struct{}),
		backoff:   make(map[uint32]time.Time),
	}
	return fc
}

func (c *FindCache) Get(orgId uint32, pattern string) ([]*Node, bool) {
	c.RLock()
	cache, ok := c.cache[orgId]
	c.RUnlock()
	if !ok {
		findCacheMiss.Inc()
		return nil, false
	}
	nodes, ok := cache.Get(pattern)
	if !ok {
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
		// dont init the cache if we are in backoff mode.
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
		c.newSeries[orgId] = make(chan struct{}, findCacheInvalidateQueue)
		c.Unlock()
	}
	cache.Add(pattern, nodes)
}

func (c *FindCache) Purge(orgId uint32) {
	c.RLock()
	cache, ok := c.cache[orgId]
	c.RUnlock()
	if !ok {
		return
	}
	cache.Purge()
}

func (c *FindCache) InvalidateFor(orgId uint32, path string) {
	c.RLock()
	cache, ok := c.cache[orgId]
	c.RUnlock()
	if !ok || cache.Len() < 1 {
		return
	}

	select {
	case c.newSeries[orgId] <- struct{}{}:
	default:
		c.Lock()
		c.backoff[orgId] = time.Now().Add(findCacheBackoff)
		delete(c.cache, orgId)
		c.Unlock()
		for i := 0; i < len(c.newSeries[orgId]); i++ {
			select {
			case <-c.newSeries[orgId]:
			default:
			}
		}
		log.Infof("memory-idx: findCache invalidate-queue full. Disabling cache for %s. num-cached-entries=%d", findCacheBackoff.String(), cache.Len())
		return
	}

	tree := &Tree{
		Items: map[string]*Node{
			"": {
				Path:     "",
				Children: make([]string, 0),
				Defs:     make([]schema.MKey, 0),
			},
		},
	}
	pos := strings.Index(path, ".")
	prevPos := 0
	for {
		branch := path[:pos]
		// add as child of parent branch
		tree.Items[path[:prevPos]].Children = []string{branch}

		// create this branch/leaf
		tree.Items[branch] = &Node{
			Path: branch,
		}
		if branch == path {
			tree.Items[branch].Defs = []schema.MKey{{}}
			break
		}
		prevPos = pos
		pos = pos + strings.Index(path[pos+1:], ".")
	}

	for _, k := range cache.Keys() {
		matches, err := find(tree, k.(string))
		if err != nil {
			log.Errorf("memory-idx: checking if new series matches expressions in findCache. series=%s expr=%s", path, k)
			continue
		}
		if len(matches) > 0 {
			cache.Remove(k)
		}
	}
	select {
	case <-c.newSeries[orgId]:
	default:
	}
}