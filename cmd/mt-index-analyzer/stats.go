package main

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/grafana/metrictank/conf"
)

var (
	totalPointsInTank  uint64
	totalPointsInStore uint64
	totalSeries        uint64

	dist       sync.Mutex
	irDist     []int
	aggDist    []int
	schemaDist []int
	comboDist  map[combo]int
)

type combo struct {
	irID     uint16
	aggID    uint16
	schemaID uint16
}

func addStat(aggID, irID, schemaID uint16, pointsInTank, pointsInStore uint64) {

	c := combo{
		irID:     irID,
		aggID:    aggID,
		schemaID: schemaID,
	}
	dist.Lock()
	aggDist[aggID]++
	irDist[irID]++
	schemaDist[schemaID]++
	comboDist[c]++
	dist.Unlock()

	atomic.AddUint64(&totalPointsInTank, pointsInTank)
	prev := atomic.LoadUint64(&totalPointsInStore)
	atomic.AddUint64(&totalPointsInStore, pointsInStore)
	after := atomic.LoadUint64(&totalPointsInStore)
	if after < prev {
		panic("counter overflowed.")
	}

	atomic.AddUint64(&totalSeries, 1)
}

func printStats() {
	fmt.Println("total points in tank:", atomic.LoadUint64(&totalPointsInTank))
	fmt.Println("total points in store:", atomic.LoadUint64(&totalPointsInStore))
	fmt.Println("total series seen:", atomic.LoadUint64(&totalSeries))

	fmt.Println("# index-rules distribution")
	for i, count := range irDist {
		ir := indexRules.Get(uint16(i))
		fmt.Printf("  %25s %40q %45s %d\n", ir.Name, ir.Pattern, ir.MaxStale, count)
	}
	fmt.Println("# aggregation-schemas distribution")
	for i, count := range aggDist {
		agg := aggregations.Get(uint16(i))
		fmt.Printf("  %25s %40q %45s %d\n", agg.Name, agg.Pattern, conf.MethodsString(agg.AggregationMethod), count)
	}
	fmt.Println("# storage-schemas distribution")
	for i, count := range schemaDist {
		schema := schemas.Get(uint16(i))
		fmt.Printf("  %25s %40q %5d %39s %d\n", schema.Name, schema.Pattern, schema.Priority, schema.Retentions.String(), count)
	}

	fmt.Println("# combination distribution ###")
	for c, count := range comboDist {
		ir := indexRules.Get(uint16(c.irID))
		agg := aggregations.Get(uint16(c.aggID))
		schema := schemas.Get(uint16(c.schemaID))
		fmt.Printf("schema(%15s/%25q) agg(%15s/%25q) ir(%15s/%25q) %d\n", schema.Name, schema.Pattern, agg.Name, agg.Pattern, ir.Name, ir.Pattern, count)
	}
}
