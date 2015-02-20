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
	"strconv"
	"strings"
	"sync"
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
}

type metricCache struct {
	raw *cacheRaw
	aggr *cacheAggr
}

type cacheRaw struct {
	data []string
	flushTime int64
}

type cacheAggr struct {
	data *aggrData
	flushTime int64
}

type aggrData struct {
	avg []int
	min []int
	max []int
}

func buildMetricDefCache() *metricCache {
	c := &metricCache{}
	c.raw = &cacheRaw{}
	c.aggr = &cacheAggr{}
	c.aggr.data = &aggrData{}
	return c
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
	carbon, err := graphite.NewGraphite("graphite-api", 2003)
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
		id := fmt.Sprintf("%d.%s", m["account"], m["name"])
		metricDefs.m.RLock()
		// Normally I would use defer unlock, but here we might need to
		// release the r/w lock and take an exclusive lock, so we have
		// to be more explicit about it.
		md, ok := metricDefs.mdefs[id]
		if !ok {
			def, err := metricdef.GetMetricDefinition(id)
			if err != nil {
				metricDefs.m.RUnlock()
				return err
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
		if err := storeMetric(m); err != nil {
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
	delete(metricDefs.mdefs, metric.ID)
	return nil
}

func storeMetric(m map[string]interface{}) error {
	if val, ok := m["value"].(float64); ok {
		b := graphite.NewMetric(fmt.Sprintf("%d.%s", m["account"], m["name"]), strconv.FormatFloat(val, 'f', -1, 64), int64(math.Floor(m["time"].(float64))))
		bufCh <- b
	}

	return nil
}

func processBuffer(c <-chan graphite.Metric, carbon *graphite.Graphite) {
	buf := make([]graphite.Metric, 0)

	t := time.NewTicker(time.Minute * 10)
	for {
		select {
		case b := <- c:
			buf = append(buf, b)
		case <- t.C:
			// A possibility: it might be worth it to hack up the
			// carbon lib to allow batch submissions of metrics if
			// doing them individually proves to be too slow
			for _, m := range buf {
				err := carbon.SendMetric(m)
				if err != nil {
					log.Println(err)
				}
			}
		}
	}
}

func rollupRaw(m map[string]interface{}) {

}

func checkThresholds(m map[string]interface{}) {

}
