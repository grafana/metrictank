package main

import (
	"bosun.org/graphite"
	"bufio"
	"fmt"
	"math"
	"net/http"
	"os"
	"strconv"
	"sync"
)

func writeErrors(curTs int64, met stat, series graphite.Response, debug bool, errs *[]string) {
	if len(*errs) != 0 && debug {
		f, err := os.Create(fmt.Sprintf("errors-%v-%d", met.def.Name, curTs))
		if err != nil {
			panic(err)
		}
		defer f.Close()
		w := bufio.NewWriter(f)
		for _, e := range *errs {
			_, err = w.WriteString(e + "\n")
			if err != nil {
				panic(err)
			}
		}
		w.WriteString("graphite response:\n")
		for _, serie := range series {
			w.WriteString(serie.Target)
			for _, p := range serie.Datapoints {
				w.WriteString(fmt.Sprintf("%s:%s\n", p[0], p[1]))
			}
		}
		w.Flush()
	}
	for _, e := range *errs {
		fmt.Println(e)
	}
}

func test(wg *sync.WaitGroup, curTs int64, met stat, host string, debug bool) {
	defer wg.Done()
	var series graphite.Response
	errs := make([]string, 0)
	defer writeErrors(curTs, met, series, debug, &errs)
	e := func(str string, pieces ...interface{}) {
		errs = append(errs, "ERROR: "+fmt.Sprintf(str, pieces...))
	}

	g := graphite.HostHeader{Host: "http://" + host + "/render", Header: http.Header{}}
	g.Header.Add("X-Org-Id", strconv.FormatInt(int64(met.def.OrgId), 10))
	g.Header.Set("User-Agent", "graphite-watcher")
	q := graphite.Request{Targets: []string{met.def.Name}}
	series, err := g.Query(&q)
	if err != nil {
		e("querying graphite: %q", err)
		return
	}
	for _, serie := range series {
		if met.def.Name != serie.Target {
			e("name %q != target name%q", met.def.Name, serie.Target)
		}

		lastTs := int64(0)
		oldestNull := int64(math.MaxInt64)
		if len(serie.Datapoints) == 0 {
			e("series for %q contains no points!", met.def.Name)
		}
		for _, p := range serie.Datapoints {
			ts, err := p[1].Int64()
			if err != nil {
				e("could not parse timestamp %q", p)
			}
			if ts <= lastTs {
				e("timestamp %v must be bigger than last %v", ts, lastTs)
			}
			if lastTs == 0 && (ts < curTs-24*3600-60 || ts > curTs-24*3600+60) {
				e("first point %q should have been about 24h ago, i.e. around %d", p, curTs-24*3600)
			}
			if lastTs != 0 && ts != lastTs+int64(met.def.Interval) {
				e("point %v is not interval %v apart from previous point", p, met.def.Interval)
			}
			_, err = p[0].Float64()
			if err != nil && ts > met.firstSeen {
				if ts < oldestNull {
					oldestNull = ts
				}
				if ts < curTs-30 {
					nullPoints.Inc(1)
					e("%v at %d : seeing a null for ts %v", met.def.Name, curTs, p[1])
				}
			} else {
				// we saw a valid point, so reset oldestNull.
				oldestNull = int64(math.MaxInt64)
			}
			lastTs = ts
		}
		if lastTs < curTs-int64(met.def.Interval) || lastTs > curTs+int64(met.def.Interval) {
			e("last point at %d is out of range. should have been around %d (now) +- %d", lastTs, curTs, met.def.Interval)
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
