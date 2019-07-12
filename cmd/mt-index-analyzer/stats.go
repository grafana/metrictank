package main

import (
	"fmt"
	"math"
	"sync"
	"sync/atomic"

	"github.com/grafana/metrictank/conf"
)

var (
	totalPointsInTank  uint64
	totalPointsInStore uint64
	totalSeries        uint64

	dist                 sync.Mutex
	irDist               []int
	aggDist              []int
	schemaDist           []int
	comboDist            map[combo]int
	pointsInTankPerPart  []uint64
	pointsInStorePerPart []uint64
	seriesPerPart        []uint64
)

type combo struct {
	irID     uint16
	aggID    uint16
	schemaID uint16
}

func addStat(part int32, aggID, irID, schemaID uint16, pointsInTank, pointsInStore uint64) {

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
	pointsInTankPerPart[part] += pointsInTank
	pointsInStorePerPart[part] += pointsInStore
	seriesPerPart[part]++
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

	fmt.Println("# combination distribution")
	for c, count := range comboDist {
		ir := indexRules.Get(uint16(c.irID))
		agg := aggregations.Get(uint16(c.aggID))
		schema := schemas.Get(uint16(c.schemaID))
		fmt.Printf("schema(%15s/%25q) agg(%15s/%25q) ir(%15s/%25q) %d\n", schema.Name, schema.Pattern, agg.Name, agg.Pattern, ir.Name, ir.Pattern, count)
	}

	fmt.Println("# distribution (across partitions) health - only accurate for when O(series per partition) >> O(partitions)")
	cov, pctDiff := stats(pointsInTankPerPart)
	fmt.Printf("points in tank cov=%.3f, diff=%.2f%%\n", cov, pctDiff)
	cov, pctDiff = stats(pointsInStorePerPart)
	fmt.Printf("points in store cov=%.3f, diff=%.2f%%\n", cov, pctDiff)
	cov, pctDiff = stats(seriesPerPart)
	fmt.Printf("series          cov=%.3f, diff=%.2f%%\n", cov, pctDiff)
}

// stats computes the stats of the distribution:
// 1) coefficient of variation, which is an excellent measure of relative variation.
// 2) percent difference between highest and lowest partition
func stats(distributions []uint64) (float64, float64) {
	var sum uint64
	min := uint64(math.MaxUint64)
	max := uint64(0)
	for _, cnt := range distributions {
		if cnt < min {
			min = cnt
		}
		if cnt > max {
			max = cnt
		}
		sum += cnt
	}
	mean := float64(sum / uint64(len(distributions))) // needs sufficiently high sum for accuracy

	var stdev float64
	for _, cnt := range distributions {
		stdev += math.Pow(float64(cnt)-mean, 2)
	}
	stdev = math.Sqrt(stdev / float64(len(distributions)))
	cov := stdev / mean
	pctdiff := float64(100*(max-min)) / float64(min)
	return cov, pctdiff
}
