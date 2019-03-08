package memory

import (
	"strings"
	"sync"

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
)

type FindCache struct {
	cache map[uint32]*lru.Cache
	size  int
	sync.RWMutex
}

func NewFindCache(size int) *FindCache {
	return &FindCache{
		cache: make(map[uint32]*lru.Cache),
		size:  size,
	}
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
	c.RUnlock()
	var err error
	if !ok {
		cache, err = lru.New(c.size)
		if err != nil {
			log.Errorf("memory-idx: findCache failed to create lru. err=%s", err)
			return
		}
		c.Lock()
		c.cache[orgId] = cache
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
	tree := &Tree{
		Items: map[string]*Node{
			"": &Node{
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
}
