package main

import (
	"bosun.org/graphite"
	"bufio"
	"fmt"
	"math"
	"net/http"
	"os"
	"strconv"
)

func writeErrors(curTs int64, met stat, series *graphite.Response, debug bool, errs *[]string) {
	if len(*errs) != 0 && debug {
		f, err := os.Create(fmt.Sprintf("errors/%v-%d", met.def.Name, curTs))
		if err != nil {
			panic(err)
		}
		defer f.Close()
		w := bufio.NewWriter(f)
		for _, e := range *errs {
			_, err = w.WriteString(fmt.Sprintf("ERROR %s\n", e))
			if err != nil {
				panic(err)
			}
		}
		w.WriteString("graphite response:\n")
		for _, serie := range *series {
			w.WriteString(serie.Target)
			for _, p := range serie.Datapoints {
				w.WriteString(fmt.Sprintf("%s:%s\n", p[1], p[0]))
			}
		}
		w.Flush()
	}
	for _, e := range *errs {
		fmt.Println(fmt.Sprintf("ERROR %d %s", curTs, e))
	}
}

func test(curTs int64, met stat, host string, debug bool) {
	var series graphite.Response
	errs := make([]string, 0)
	defer writeErrors(curTs, met, &series, debug, &errs)
	e := func(str string, pieces ...interface{}) {
		errs = append(errs, fmt.Sprintf(str, pieces...))
	}

	g := graphite.HostHeader{Host: "http://" + host + "/render", Header: http.Header{}}
	g.Header.Add("X-Org-Id", strconv.FormatInt(int64(met.def.OrgId), 10))
	g.Header.Set("User-Agent", "graphite-watcher")
	q := graphite.Request{Targets: []string{met.def.Name}}
	series, err := g.Query(&q)
	if err != nil {
		e("querying graphite: %v", err)
		return
	}
	interval := 600 // we request 24h worth of data. returned data will be consolidated.
	for _, serie := range series {
		if met.def.Name != serie.Target {
			e("%v : bad target name %v", met.def.Name, serie.Target)
		}

		lastTs := int64(0)
		oldestNull := int64(math.MaxInt64)
		if len(serie.Datapoints) == 0 {
			e("%v : series contains no points!", met.def.Name)
		}
		for _, p := range serie.Datapoints {
			ts, err := p[1].Int64()
			if err != nil {
				e("%v : could not parse timestamp %q", met.def.Name, p)
			}
			if ts <= lastTs {
				e("%v timestamp %v must be bigger than last %v", met.def.Name, ts, lastTs)
			}
			if lastTs == 0 && (ts < curTs-24*3600-60 || ts > curTs-24*3600+60) {
				e("%v first point %q should have been about 24h ago, i.e. around %d", met.def.Name, p, curTs-24*3600)
			}
			if lastTs != 0 && ts != lastTs+int64(interval) {
				e("%v point %v is not interval %v apart from previous point", met.def.Name, p, interval)
			}
			_, err = p[0].Float64()
			if err != nil && ts > met.firstSeen {
				if ts < oldestNull {
					oldestNull = ts
				}
				if ts < curTs-30 {
					nullPoints.Inc(1)
					e("%v : seeing a null for ts %v", met.def.Name, p[1])
				}
			} else {
				// we saw a valid point, so reset oldestNull.
				oldestNull = int64(math.MaxInt64)
			}
			lastTs = ts
		}
		if lastTs < curTs-int64(interval) || lastTs > curTs+int64(interval) {
			e("%v : last point at %d is out of range. should have been around %d (now) +- %d", met.def.Name, lastTs, curTs, interval)
		}
		// if there was no null, we treat the point after the last one we had as null
		if oldestNull == math.MaxInt64 {
			oldestNull = lastTs + int64(interval)
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
