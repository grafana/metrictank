package main

/*
Some important libs that have turned up - may or may not be in this file:
https://github.com/streadway/amqp -- rabbitmq
https://github.com/mattbaird/elastigo -- elasticsearch
https://github.com/marpaia/graphite-golang -- carbon
*/
import (
	"encoding/json"
	"fmt"
	"github.com/marpaia/graphite-golang"
	"github.com/raintank/raintank-metric/qproc"
	"github.com/raintank/raintank-metric/metricdef"
	"github.com/streadway/amqp"
	"log"
	"math"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

type publisher struct {
	*amqp.Channel
}

type metricDefCache struct {
	mdefs map[string]*metricDef
	m sync.RWMutex
}

// Fill this out once it's clear what should be in here
type metricDef struct {
	mdef *metricdef.MetricDefinition
	cache *metricCache
	m sync.RWMutex
}

type metricCache struct {
	raw *cacheRaw
	aggr *cacheAggr
}

type cacheRaw struct {
	data []float64
	flushTime int64
}

type cacheAggr struct {
	data *aggrData
	flushTime int64
}

type aggrData struct {
	avg []*float64
	min []*float64
	max []*float64
}

func buildMetricDefCache() *metricCache {
	c := &metricCache{}
	c.raw = &cacheRaw{}
	c.aggr = &cacheAggr{}
	c.aggr.data = &aggrData{}
	return c
}

type indvMetric struct {
	id string
	account int
	name string
	metric string
	location string
	interval int
	value float64
	valReal bool
	unit string
	time int64
	site int
	monitor int
	targetType string
}

var metricDefs *metricDefCache

type PayloadProcessor func(*publisher, *amqp.Delivery) error

// dev var declarations, until real config/flags are added
var rabbitURL string = "amqp://rabbitmq"
var bufCh chan graphite.Metric

func init() {
	metricDefs = &metricDefCache{}
	metricDefs.mdefs = make(map[string]*metricDef)
	bufCh = make(chan graphite.Metric, 0) // unbuffered for now, will buffer later
	// currently using both a) hard-coded values for the server and b) using
	// the graphite client instead of influxdb's client to connect here.
	// Using graphite instead of influxdb should be more flexible, at least
	// initially.
	carbon, err := graphite.NewGraphite("influxdb", 2003)
	if err != nil {
		panic(err)
	}
	go processBuffer(bufCh, carbon)
}

func main() {
	// First fire up a queue to consume metric def events
	mdConn, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	defer mdConn.Close()
	log.Println("connected")

	done := make(chan error, 1)
	
	// create a publisher
	pub, err := qproc.CreatePublisher(mdConn, "metricEvents", "fanout")
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	/*
	testProc := func(pub *qproc.Publisher, d *amqp.Delivery) error {
		fmt.Printf("Got us a queue item: %d B, [%v], %q :: %+v\n", len(d.Body), d.DeliveryTag, d.Body, d)
		e := d.Ack(false)
		if e != nil {
			return e
		}
		return nil
	}
	*/

	err = qproc.ProcessQueue(mdConn, nil, "metrics", "topic", "metrics.*", "", done, processMetricDefEvent)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}
	err = qproc.ProcessQueue(mdConn, pub, "metricResults", "x-consistent-hash", "10", "", done, processMetrics)
	if err != nil {
		log.Println(err)
		os.Exit(1)
	}

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGQUIT)
		buf := make([]byte, 1<<20)
		for {
			<-sigs
			runtime.Stack(buf, true)
			log.Printf("=== received SIGQUIT ===\n*** goroutine dump...\n%s\n*** end\n", buf)
		}
	}()

	err = <- done
	fmt.Println("all done!")
	if err != nil {
		log.Printf("Had an error, aiiieeee! '%s'", err.Error())
	}
}

func processMetrics(pub *qproc.Publisher, d *amqp.Delivery) error {
	metrics := make([]map[string]interface{}, 0)
	if err := json.Unmarshal(d.Body, &metrics); err != nil {
		return err
	}

	fmt.Printf("The parsed out json: %v\n", metrics)

	for _, m := range metrics {
		fmt.Printf("would process %s\n", m["name"])
		id := fmt.Sprintf("%d.%s", int64(m["account"].(float64)), m["name"])
		metricDefs.m.RLock()
		// Normally I would use defer unlock, but here we might need to
		// release the r/w lock and take an exclusive lock, so we have
		// to be more explicit about it.
		if md, ok := metricDefs.mdefs[id]; !ok {
			log.Printf("adding %s to metric defs", id)
			def, err := metricdef.GetMetricDefinition(id)
			if err != nil  {
				if err.Error() != "record not found" {
					// create a new metric
					log.Println("creating new metric")
					def, err = metricdef.NewFromMessage(m)
					if err != nil {
						metricDefs.m.RUnlock()
						return err
					}
				} else {
					metricDefs.m.RUnlock()
					return err
				}
			}
			md = &metricDef{mdef: def}
			md.cache = buildMetricDefCache()
			now := time.Now().Unix()
			md.cache.raw.flushTime = now - 600
			md.cache.aggr.flushTime = now - 26100
			metricDefs.m.RUnlock()
			metricDefs.m.Lock()
			metricDefs.mdefs[id] = md
			metricDefs.m.Unlock()
		}
		if err := storeMetric(m, pub); err != nil {
			return err
		}
	}

	if err := d.Ack(false); err != nil {
		return err
	}
	return nil
}

func processMetricDefEvent(pub *qproc.Publisher, d *amqp.Delivery) error {
	action := strings.Split(d.RoutingKey, ".")[1]
	// This should be able to craft the metric directly from the JSON, but
	// waiting on that until we see this in action
	switch action {
	case "update":
		metric, err := metricdef.DefFromJSON(d.Body)
		if err != nil {
			return err
		}
		if err := updateMetricDef(metric); err != nil {
			return err
		}
	case "remove":
		metric, err := metricdef.DefFromJSON(d.Body)
		if err != nil {
			return err
		}
		if err := removeMetricDef(metric); err != nil {
			return err
		}
	default:
		err := fmt.Errorf("message has unknown action '%s'", action)
		return err
	}

	return nil
}

func updateMetricDef(metric *metricdef.MetricDefinition) error {
	log.Printf("Metric we have: %v :: %q", metric, metric)
	log.Printf("Updating metric def for %s", metric.ID)
	metricDefs.m.Lock()
	defer metricDefs.m.Unlock()

	md, ok := metricDefs.mdefs[metric.ID]
	md.m.RLock()
	defer md.m.RUnlock()
	newMd := &metricDef{ mdef: metric }
	if ok {
		log.Printf("metric %s found", metric.ID)
		if md.mdef.LastUpdate >= metric.LastUpdate {
			log.Printf("%s already up to date", metric.ID)
			return nil
		}
		newMd.cache = md.cache
	} else {
		log.Printf("no definition for %s found, building new cache", metric.ID)
		newMd.cache = buildMetricDefCache()
		now := time.Now().Unix()
		newMd.cache.raw.flushTime = now - 600
		newMd.cache.aggr.flushTime = now - 21600
	}
	metricDefs.mdefs[metric.ID] = newMd

	return nil
}

func removeMetricDef(metric *metricdef.MetricDefinition) error {
	log.Printf("Metric we have: %v :: %q", metric, metric)
	log.Printf("Removing metric def for %s", metric.ID)
	metricDefs.m.Lock()
	defer metricDefs.m.Unlock()
	md, ok := metricDefs.mdefs[metric.ID]; 
	if !ok {
		return nil
	}
	md.m.Lock()
	defer md.m.Unlock()

	delete(metricDefs.mdefs, metric.ID)
	return nil
}

func storeMetric(m map[string]interface{}, pub *qproc.Publisher) error {
	log.Printf("storing metric: %+v", m)
	met, err := buildIndvMetric(m)
	if err != nil {
		return err
	}
	if met.valReal {
		log.Printf("Adding to buffer")
		b := graphite.NewMetric(met.id, strconv.FormatFloat(met.value, 'f', -1, 64), met.time)
		bufCh <- b
	}
	rollupRaw(met)
	checkThresholds(met, pub)
	return nil
}

func processBuffer(c <-chan graphite.Metric, carbon *graphite.Graphite) {
	buf := make([]graphite.Metric, 0)

	t := time.NewTicker(time.Minute * 10)
	for {
		select {
		case b := <- c:
			log.Println("appending to buffer")
			buf = append(buf, b)
		case <- t.C:
			// A possibility: it might be worth it to hack up the
			// carbon lib to allow batch submissions of metrics if
			// doing them individually proves to be too slow
			log.Printf("flushing buffer now")
			for _, m := range buf {
				err := carbon.SendMetric(m)
				if err != nil {
					log.Println(err)
				}
			}
			buf = nil
		}
	}
}

func rollupRaw(met *indvMetric) {
	metricDefs.m.RLock()
	defer metricDefs.m.RUnlock()
	def := metricDefs.mdefs[met.id]
	def.m.Lock()
	defer def.m.Unlock()
	if def.cache.raw.flushTime < (met.time - 600) {
		if def.cache.aggr.flushTime < (met.time - 21600) {
			var min, max, avg, sum *float64
			count := len(def.cache.aggr.data.min)
			// not slavish; we need to manipulate three slices at
			// once
			for i := 0; i < count; i++ {
				if min == nil || *def.cache.aggr.data.min[i] < *min {
					min = def.cache.aggr.data.min[i]
				}
				if max == nil || *def.cache.aggr.data.max[i] < *max {
					max = def.cache.aggr.data.max[i]
				}
				if def.cache.aggr.data.avg[i] != nil {
					if sum == nil {
						sum = def.cache.aggr.data.avg[i]
					} else {
						*sum += *def.cache.aggr.data.avg[i]
					}
				}
			}
			if count > 0 {
				z := *sum / float64(count)
				avg = &z
			}
			def.cache.aggr.data.avg = nil
			def.cache.aggr.data.min = nil
			def.cache.aggr.data.max = nil
			def.cache.aggr.flushTime = met.time
			if avg != nil {
				log.Printf("writing 6 hour rollup for %s", met.id)
				id := fmt.Sprintf("6hour.avg.%s", met.id)
				b := graphite.NewMetric(id, strconv.FormatFloat(*avg, 'f', -1, 64), met.time)
				bufCh <- b
			}
			if min != nil {
				id := fmt.Sprintf("6hour.min.%s", met.id)
				b := graphite.NewMetric(id, strconv.FormatFloat(*min, 'f', -1, 64), met.time)
				bufCh <- b
			}
			if max != nil {
				id := fmt.Sprintf("6hour.max.%s", met.id)
				b := graphite.NewMetric(id, strconv.FormatFloat(*max, 'f', -1, 64), met.time)
				bufCh <- b
			}
		}
		var min, max, avg, sum *float64
		count := len(def.cache.raw.data)
		for _, p := range def.cache.raw.data {
			if min == nil || p < *min {
				min = &p
			}
			if max == nil || p < *max {
				max = &p
			}
			if sum == nil {
				sum = &p
			} else {
				*sum += p
			}
		}
		if count > 0 {
			z := *sum / float64(count)
			avg = &z
		}
		if avg != nil {
			log.Printf("writing 10 min rollup for %s:%f", met.id, *avg)
			id := fmt.Sprintf("10min.avg.%s", met.id)
			b := graphite.NewMetric(id, strconv.FormatFloat(*avg, 'f', -1, 64), met.time)
			bufCh <- b
		}
		if min != nil {
			id := fmt.Sprintf("10min.min.%s", met.id)
			b := graphite.NewMetric(id, strconv.FormatFloat(*min, 'f', -1, 64), met.time)
			bufCh <- b
		}
		if max != nil {
			id := fmt.Sprintf("10min.max.%s", met.id)
			b := graphite.NewMetric(id, strconv.FormatFloat(*max, 'f', -1, 64), met.time)
			bufCh <- b
		}
		def.cache.aggr.data.min = append(def.cache.aggr.data.min, min)
		def.cache.aggr.data.max = append(def.cache.aggr.data.max, max)
		def.cache.aggr.data.avg = append(def.cache.aggr.data.avg, avg) 
	}
	if met.valReal {
		def.cache.raw.data = append(def.cache.raw.data, met.value)
	}
}

func checkThresholds(met *indvMetric, pub *qproc.Publisher) {
	metricDefs.m.RLock()
	defer metricDefs.m.RUnlock()
	def := metricDefs.mdefs[met.id]
	def.m.Lock()
	defer def.m.Unlock()
}

func buildIndvMetric(m map[string]interface{}) (*indvMetric, error) {
	id := fmt.Sprintf("%d.%s", int64(m["account"].(float64)), m["name"])
	// TODO: validations.
	var valReal bool
	var val float64

	if v, exists := m["value"]; exists {
		if vf, ok := v.(float64); ok {
			val = vf
			valReal = true
		}
	}

	met := &indvMetric{id: id,
		account: int(m["account"].(float64)),
		name: m["name"].(string),
		metric: m["metric"].(string),
		location: m["location"].(string),
		interval: int(m["interval"].(float64)),
		value: val,
		valReal: valReal,
		unit: m["unit"].(string),
		time: int64(math.Floor(m["time"].(float64))),
		site: int(m["site"].(float64)),
		monitor: int(m["monitor"].(float64)),
		targetType: m["target_type"].(string)}
	return met, nil
}
