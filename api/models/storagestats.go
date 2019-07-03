package models

import (
	"strconv"

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

func (ss StorageStats) MarshalJSONFast(b []byte) ([]byte, error) {
	b = append(b, '{')
	b, _ = ss.MarshalJSONFastRaw(b)
	b = append(b, '}')
	return b, nil
}

func (ss StorageStats) MarshalJSONFastRaw(b []byte) ([]byte, error) {
	b = append(b, `"executeplan.cache-miss.count":`...)
	b = strconv.AppendUint(b, uint64(ss.CacheMiss), 10)
	b = append(b, `,"executeplan.cache-hit-partial.count":`...)
	b = strconv.AppendUint(b, uint64(ss.CacheHitPartial), 10)
	b = append(b, `,"executeplan.cache-hit.count":`...)
	b = strconv.AppendUint(b, uint64(ss.CacheHit), 10)
	b = append(b, `,"executeplan.chunks-from-tank.count":`...)
	b = strconv.AppendUint(b, uint64(ss.ChunksFromTank), 10)
	b = append(b, `,"executeplan.chunks-from-cache.count":`...)
	b = strconv.AppendUint(b, uint64(ss.ChunksFromCache), 10)
	b = append(b, `,"executeplan.chunks-from-store.count":`...)
	b = strconv.AppendUint(b, uint64(ss.ChunksFromStore), 10)
	return b, nil
}

func StorageStatsSum(a, b StorageStats) StorageStats {
	return StorageStats{
		CacheMiss:       a.CacheMiss + b.CacheMiss,
		CacheHitPartial: a.CacheHitPartial + b.CacheHitPartial,
		CacheHit:        a.CacheHit + b.CacheHit,
		ChunksFromTank:  a.ChunksFromTank + b.ChunksFromTank,
		ChunksFromCache: a.ChunksFromCache + b.ChunksFromCache,
		ChunksFromStore: a.ChunksFromStore + b.ChunksFromStore,
	}
}

func (ss StorageStats) Trace(span opentracing.Span) {
	span.SetTag("cache-miss", ss.CacheMiss)
	span.SetTag("cache-hit-partial", ss.CacheHitPartial)
	span.SetTag("cache-hit", ss.CacheHit)
	span.SetTag("chunks-from-tank", ss.ChunksFromTank)
	span.SetTag("chunks-from-cache", ss.ChunksFromCache)
	span.SetTag("chunks-from-store", ss.ChunksFromStore)
}
