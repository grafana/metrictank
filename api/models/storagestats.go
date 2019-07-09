package models

import (
	"strconv"
	"sync/atomic"

	"github.com/grafana/metrictank/mdata/cache"
	opentracing "github.com/opentracing/opentracing-go"
)

//go:generate msgp
type StorageStats struct {
	CacheMiss       uint32 `json:"executeplan.cache-miss.count"`
	CacheHitPartial uint32 `json:"executeplan.cache-hit-partial.count"`
	CacheHit        uint32 `json:"executeplan.cache-hit.count"`
	ChunksFromTank  uint32 `json:"executeplan.chunks-from-tank.count"`
	ChunksFromCache uint32 `json:"executeplan.chunks-from-cache.count"`
	ChunksFromStore uint32 `json:"executeplan.chunks-from-store.count"`
}

func (ss *StorageStats) IncCacheResult(t cache.ResultType) {
	switch t {
	case cache.Hit:
		atomic.AddUint32(&ss.CacheHit, 1)
	case cache.HitPartial:
		atomic.AddUint32(&ss.CacheHitPartial, 1)
	case cache.Miss:
		atomic.AddUint32(&ss.CacheMiss, 1)
	}
}

func (ss *StorageStats) IncChunksFromTank(n uint32) {
	atomic.AddUint32(&ss.ChunksFromTank, n)
}
func (ss *StorageStats) IncChunksFromCache(n uint32) {
	atomic.AddUint32(&ss.ChunksFromCache, n)
}
func (ss *StorageStats) IncChunksFromStore(n uint32) {
	atomic.AddUint32(&ss.ChunksFromStore, n)
}

// Add adds a to ss.
func (ss *StorageStats) Add(a *StorageStats) {
	atomic.AddUint32(&ss.CacheHit, atomic.LoadUint32(&a.CacheHit))
	atomic.AddUint32(&ss.CacheHitPartial, atomic.LoadUint32(&a.CacheHitPartial))
	atomic.AddUint32(&ss.CacheMiss, atomic.LoadUint32(&a.CacheMiss))
	atomic.AddUint32(&ss.ChunksFromTank, atomic.LoadUint32(&a.ChunksFromTank))
	atomic.AddUint32(&ss.ChunksFromCache, atomic.LoadUint32(&a.ChunksFromCache))
	atomic.AddUint32(&ss.ChunksFromStore, atomic.LoadUint32(&a.ChunksFromStore))
}

func (ss *StorageStats) MarshalJSONFast(b []byte) ([]byte, error) {
	b = append(b, '{')
	b, _ = ss.MarshalJSONFastRaw(b)
	b = append(b, '}')
	return b, nil
}

func (ss *StorageStats) MarshalJSONFastRaw(b []byte) ([]byte, error) {
	b = append(b, `"executeplan.cache-miss.count":`...)
	b = strconv.AppendUint(b, uint64(atomic.LoadUint32(&ss.CacheMiss)), 10)
	b = append(b, `,"executeplan.cache-hit-partial.count":`...)
	b = strconv.AppendUint(b, uint64(atomic.LoadUint32(&ss.CacheHitPartial)), 10)
	b = append(b, `,"executeplan.cache-hit.count":`...)
	b = strconv.AppendUint(b, uint64(atomic.LoadUint32(&ss.CacheHit)), 10)
	b = append(b, `,"executeplan.chunks-from-tank.count":`...)
	b = strconv.AppendUint(b, uint64(atomic.LoadUint32(&ss.ChunksFromTank)), 10)
	b = append(b, `,"executeplan.chunks-from-cache.count":`...)
	b = strconv.AppendUint(b, uint64(atomic.LoadUint32(&ss.ChunksFromCache)), 10)
	b = append(b, `,"executeplan.chunks-from-store.count":`...)
	b = strconv.AppendUint(b, uint64(atomic.LoadUint32(&ss.ChunksFromStore)), 10)
	return b, nil
}

func (ss *StorageStats) Trace(span opentracing.Span) {
	span.SetTag("cache-miss", atomic.LoadUint32(&ss.CacheMiss))
	span.SetTag("cache-hit-partial", atomic.LoadUint32(&ss.CacheHitPartial))
	span.SetTag("cache-hit", atomic.LoadUint32(&ss.CacheHit))
	span.SetTag("chunks-from-tank", atomic.LoadUint32(&ss.ChunksFromTank))
	span.SetTag("chunks-from-cache", atomic.LoadUint32(&ss.ChunksFromCache))
	span.SetTag("chunks-from-store", atomic.LoadUint32(&ss.ChunksFromStore))
}
