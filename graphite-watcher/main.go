package main

import (
	"github.com/Dieterbe/go-metrics"
	"github.com/raintank/raintank-metric/schema"
	"log"
	"math/rand"
	"net"
	"os"
	"sync"
	"time"
	//"strings"
)

func perror(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

type stat struct {
	def       schema.MetricDefinition
	firstSeen int64
}

var targets = make(map[string]stat)
var targetKeys = make([]string, 0)
var targetsLock sync.Mutex

var lag = metrics.NewHistogram(metrics.NewExpDecaySample(1028, 0.015))
var numMetrics = metrics.NewGauge()
var nullPoints = metrics.NewCounter()

func main() {
	if len(os.Args) != 4 {
		log.Fatal("usage: graphite-watcher <elasticsearch-addr> <metrics-addr> <graphite-addr>")
	}
	addr, _ := net.ResolveTCPAddr("tcp", os.Args[2])
	go metrics.Graphite(metrics.DefaultRegistry, 10e9, "graphite-watcher.", addr)
	metrics.Register("lag", lag)
	metrics.Register("num_metrics", numMetrics)
	metrics.Register("null_points", nullPoints)

	// for a metric to exist in ES at t=Y, there must at least have been 1 point for that metric
	// at a time X where X < Y.  Hence, we can confidently say that if we see a metric at Y, we can
	// demand data to show up for that metric at >=Y
	// for our data check to be useful we need metrics to show up in ES soon after being in the pipeline,
	// which seems to be true (see nsqadmin)
	go func() {
		getEsTick := time.NewTicker(time.Second * time.Duration(10))
		for range getEsTick.C {
			metrics := getMetrics(os.Args[1])
			numMetrics.Update(int64(len(metrics)))
			tsUnix := time.Now().Unix()
			for _, met := range metrics {
				targetsLock.Lock()
				if _, ok := targets[met.Name]; !ok {
					targetKeys = append(targetKeys, met.Name)
					targets[met.Name] = stat{met, tsUnix}
				}
				targetsLock.Unlock()
			}
		}
	}()

	tick := time.NewTicker(time.Millisecond * time.Duration(100))
	wg := &sync.WaitGroup{}
	for ts := range tick.C {
		targetsLock.Lock()
		if len(targetKeys) > 0 {
			key := targetKeys[rand.Intn(len(targets))]
			wg.Add(1)
			go test(wg, ts.Unix(), targets[key], os.Args[3])
		}
		targetsLock.Unlock()
	}
	wg.Wait()
}
