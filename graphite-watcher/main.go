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
	"os"
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

type stat struct {
	def       schema.MetricDefinition
	firstSeen int64
}

var targets map[string]stat
var targetKeys []string
var targetsLock sync.Mutex

func init() {
	targets = make(map[string]stat)
	targetKeys = make([]string, 0)
}

func main() {
	if len(os.Args) != 4 {
		log.Fatal("usage: graphite-watcher <elasticsearch-addr> <metrics-addr> <graphite-addr>")
	}
	addr, _ := net.ResolveTCPAddr("tcp", os.Args[2])
	go metrics.Graphite(metrics.DefaultRegistry, 10e9, "graphite-watcher.", addr)
	lag := metrics.NewHistogram(metrics.NewExpDecaySample(1028, 0.015))
	metrics.Register("lag", lag)
	numMetrics := metrics.NewGauge()
	metrics.Register("num_metrics", numMetrics)
	nullPoints := metrics.NewCounter()
	metrics.Register("null_points", nullPoints)

	// for a metric to exist in ES at t=Y, there must at least have been 1 point for that metric
	// at a time X where X < Y.  Hence, we can confidently say that if we see a metric at Y, we can
	// demand data to show up for that metric at >=Y
	// for our data check to be useful we need metrics to show up in ES soon after being in the pipeline,
	// which seems to be true (see nsqadmin)
	updateTargets := func() {
		getEsTick := time.NewTicker(time.Second * time.Duration(10))
		for range getEsTick.C {
			res, err := http.Get("http://" + os.Args[1] + "/metric/_search?q=*:*&size=10000000")
			perror(err)
			defer res.Body.Close()
			body, err := ioutil.ReadAll(res.Body)
			perror(err)
			var data EsResult
			tsUnix := time.Now().Unix()
			err = json.Unmarshal(body, &data)
			perror(err)
			amount := len(data.Hits.Hits)
			numMetrics.Update(int64(amount))
			for _, h := range data.Hits.Hits {
				//fmt.Println(h.Source.Name)
				targetsLock.Lock()
				if _, ok := targets[h.Source.Name]; !ok {
					targets[h.Source.Name] = stat{h.Source, tsUnix}
					targetKeys = append(targetKeys, h.Source.Name)
				}
				targetsLock.Unlock()
			}
		}
	}

	test := func(wg *sync.WaitGroup, curTs int64, met stat) {
		defer wg.Done()
		g := graphite.HostHeader{Host: "http://" + os.Args[3] + "/render", Header: http.Header{}}
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
					fmt.Println("ERROR: first point", p, "should have been about 24h ago")
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
				fmt.Println("ERROR: last point at ", lastTs, "is out of range")
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

	go updateTargets()

	tick := time.NewTicker(time.Millisecond * time.Duration(100))
	wg := &sync.WaitGroup{}
	for ts := range tick.C {
		var t stat
		targetsLock.Lock()
		if len(targetKeys) > 0 {
			key := targetKeys[rand.Intn(len(targets))]
			t = targets[key]
		}
		targetsLock.Unlock()
		if len(targetKeys) > 0 {
			wg.Add(1)
			go test(wg, ts.Unix(), t)
		}
	}
	wg.Wait()
}
