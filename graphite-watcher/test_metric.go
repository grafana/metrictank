package main

import (
	"bosun.org/graphite"
	"fmt"
	"log"
	"math"
	"net/http"
	"strconv"
	"sync"
)

func test(wg *sync.WaitGroup, curTs int64, met stat, host string) {
	defer wg.Done()
	g := graphite.HostHeader{Host: "http://" + host + "/render", Header: http.Header{}}
	g.Header.Add("X-Org-Id", strconv.FormatInt(int64(met.def.OrgId), 10))
	g.Header.Set("User-Agent", "graphite-watcher")
	q := graphite.Request{Targets: []string{met.def.Name}}
	series, err := g.Query(&q)
	if err != nil {
		log.Println("ERROR querying graphite:", err)
		return
	}
	for _, serie := range series {
		if met.def.Name != serie.Target {
			fmt.Println("ERROR: name != target name:", met.def.Name, serie.Target)
		}

		lastTs := int64(0)
		oldestNull := int64(math.MaxInt64)
		if len(serie.Datapoints) == 0 {
			fmt.Println("ERROR: series for", met.def.Name, "contains no points!")
		}
		for _, p := range serie.Datapoints {
			ts, err := p[1].Int64()
			if err != nil {
				fmt.Println("ERROR: could not parse timestamp", p)
			}
			if ts <= lastTs {
				fmt.Println("ERROR: timestamp must be bigger than last", lastTs, ts)
			}
			if lastTs == 0 && (ts < curTs-24*3600-60 || ts > curTs-24*3600+60) {
				fmt.Println("ERROR: first point", p, "should have been about 24h ago, i.e. around", curTs-24*3600)
			}
			if lastTs != 0 && ts != lastTs+int64(met.def.Interval) {
				fmt.Println("ERROR: point", p, " is not interval ", met.def.Interval, "apart from previous point")
			}
			_, err = p[0].Float64()
			if err != nil && ts > met.firstSeen {
				if ts < oldestNull {
					oldestNull = ts
				}
				if ts < curTs-30 {
					nullPoints.Inc(1)
					fmt.Println("ERROR: ", met.def.Name, " at", curTs, "seeing a null for ts", p[1])
				}
			} else {
				// we saw a valid point, so reset oldestNull.
				oldestNull = int64(math.MaxInt64)
			}
			lastTs = ts
		}
		if lastTs < curTs-int64(met.def.Interval) || lastTs > curTs+int64(met.def.Interval) {
			fmt.Println("ERROR: last point at ", lastTs, "is out of range. should have been around", curTs, "(now) +-", met.def.Interval)
		}
		// if there was no null, we treat the point after the last one we had as null
		if oldestNull == math.MaxInt64 {
			oldestNull = lastTs + int64(met.def.Interval)
		}
		// lag cannot be < 0
		if oldestNull > curTs {
			oldestNull = curTs
		}
		// lag is from first null after a range of data until now
		//fmt.Printf("%60s - lag %d\n", name, curTs-oldestNull)
		lag.Update(curTs - oldestNull)
	}
}
