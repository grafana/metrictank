package main

import (
	"bosun.org/graphite"
	"encoding/json"
	"fmt"
	"github.com/Dieterbe/go-metrics"
	"github.com/raintank/raintank-metric/schema"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"net"
	"net/http"
	"strconv"
	"sync"
	"time"
	//"strings"
)

type Hit struct {
	Index  string                  `json:"_index"`
	Type   string                  `json:"_type"`
	Id     string                  `json:"_id"`
	Score  float64                 `json:"_score"`
	Source schema.MetricDefinition `json:"_source"`
}

type EsResult struct {
	Took     int
	TimedOut bool
	_shards  struct {
		total      int
		successful int
		failed     int
	}
	Hits struct {
		Total    int
		MaxScore int
		Hits     []Hit
	}
}

func perror(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

var targets []schema.MetricDefinition
var targetsLock sync.Mutex

func init() {
	targets = make([]schema.MetricDefinition, 0)
}

func main() {
	addr, _ := net.ResolveTCPAddr("tcp", "localhost:2003")
	go metrics.Graphite(metrics.DefaultRegistry, 10e9, "graphite-watcher.", addr)
	lag := metrics.NewHistogram(metrics.NewExpDecaySample(1028, 0.015))
	metrics.Register("lag", lag)
	numMetrics := metrics.NewGauge()
	metrics.Register("num_metrics", numMetrics)

	start := time.Now().Unix()

	updateTargets := func() {
		getEsTick := time.NewTicker(time.Second * time.Duration(10))
		for range getEsTick.C {
			res, err := http.Get("http://localhost:9200/metric/_search?q=*:*&size=10000000")
			perror(err)
			defer res.Body.Close()
			body, err := ioutil.ReadAll(res.Body)
			perror(err)
			var data EsResult
			err = json.Unmarshal(body, &data)
			perror(err)
			amount := len(data.Hits.Hits)
			numMetrics.Update(int64(amount))
			newTargets := make([]schema.MetricDefinition, amount, amount)
			for i, h := range data.Hits.Hits {
				//fmt.Println(h.Source.Name)
				newTargets[i] = h.Source
			}
			targetsLock.Lock()
			targets = newTargets
			targetsLock.Unlock()
		}
	}

	test := func(wg *sync.WaitGroup, curTs int64, name string, orgId, interval int) {
		g := graphite.HostHeader{Host: "http://localhost:8888/render", Header: http.Header{}}
		g.Header.Add("X-Org-Id", strconv.FormatInt(int64(orgId), 10))
		q := graphite.Request{Targets: []string{name}}
		series, err := g.Query(&q)
		perror(err)
		for _, serie := range series {
			if name != serie.Target {
				fmt.Println("ERROR: name != target name:", name, serie.Target)
			}

			lastTs := int64(0)
			oldestNull := int64(math.MaxInt64)
			if len(serie.Datapoints) == 0 {
				fmt.Println("ERROR: series for", name, "contains no points!")
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
					fmt.Println("ERROR: first point", p, "should have been about 24h ago")
				}
				if lastTs != 0 && ts != lastTs+int64(interval) {
					fmt.Println("ERROR: point", p, " is not interval ", interval, "apart from previous point")
				}
				_, err = p[0].Float64()
				if err != nil && ts > start {
					if ts < oldestNull {
						oldestNull = ts
					}
					if ts < curTs-30 {
						// will show up in lag metric too
						fmt.Println("ERROR: point has a recent null value", p)
					}
				}
				lastTs = ts
			}
			if lastTs < curTs-int64(interval) || lastTs > curTs+int64(interval) {
				fmt.Println("ERROR: last point at ", lastTs, "is out of range")
			}
			// if there was no null, we treat the point after the last one we had as null
			if oldestNull == math.MaxInt64 {
				oldestNull = lastTs + int64(interval)
			}
			// lag cannot be < 0
			if oldestNull > curTs {
				oldestNull = curTs
			}
			// lag is from first null, even if there were non-nulls after it
			//fmt.Printf("%60s - lag %d\n", name, curTs-oldestNull)
			lag.Update(curTs - oldestNull)
		}
		wg.Done()
	}

	go updateTargets()

	tick := time.NewTicker(time.Millisecond * time.Duration(100))
	wg := &sync.WaitGroup{}
	for ts := range tick.C {
		var t schema.MetricDefinition
		targetsLock.Lock()
		if len(targets) > 0 {
			t = targets[rand.Intn(len(targets))]
		}
		targetsLock.Unlock()
		if len(targets) > 0 {
			wg.Add(1)
			go test(wg, ts.Unix(), t.Name, t.OrgId, t.Interval)
		}
	}
	wg.Wait()
}
