package main

/*
 * Copyright (c) 2015, Raintank Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import (
	"encoding/json"
	"fmt"
	"github.com/ctdk/goas/v2/logger"
	"github.com/marpaia/graphite-golang"
	"github.com/raintank/raintank-metric/eventdef"
	"github.com/raintank/raintank-metric/metricdef"
	"github.com/raintank/raintank-metric/qproc"
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

// Monitor states
const (
	stateOK int8 = iota
	stateWarn
	stateCrit
)

// cache to hold metric definitions
type metricDefCache struct {
	mdefs map[string]*metricDef
	m     sync.RWMutex
}

// a struct to hold metric definitions and their cached information, along with
// a mutex to keep data safe from concurrent access.
type metricDef struct {
	mdef  *metricdef.MetricDefinition
	cache *metricCache
	m     sync.RWMutex
}

// Below are some structs for caching metric information.
type metricCache struct {
	raw  *cacheRaw
	aggr *cacheAggr
}

type cacheRaw struct {
	data      []float64
	flushTime int64
}

type cacheAggr struct {
	data      *aggrData
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

// Holds the information from an individual metric item coming in from
// rabbitmq
type indvMetric struct {
	id         string
	orgID      int
	name       string
	metric     string
	location   string
	interval   int
	value      float64
	valReal    bool
	unit       string
	time       int64
	siteID     int
	monitorID  int
	targetType string
}

var metricDefs *metricDefCache

var bufCh chan graphite.Metric

func init() {
	initConfig()

	var numCPU int
	if config.NumWorkers != 0 {
		numCPU = config.NumWorkers
	} else {
		numCPU = runtime.NumCPU()
	}
	runtime.GOMAXPROCS(numCPU)

	metricDefs = &metricDefCache{}
	metricDefs.mdefs = make(map[string]*metricDef)
	bufCh = make(chan graphite.Metric, numCPU)

	err := eventdef.InitElasticsearch(config.ElasticsearchDomain, config.ElasticsearchPort, config.ElasticsearchUser, config.ElasticsearchPasswd)
	if err != nil {
		panic(err)
	}
	err = metricdef.InitElasticsearch(config.ElasticsearchDomain, config.ElasticsearchPort, config.ElasticsearchUser, config.ElasticsearchPasswd)
	if err != nil {
		panic(err)
	}
	err = metricdef.InitRedis(config.RedisAddr, config.RedisPasswd, config.RedisDB)

	// currently using the graphite client instead of influxdb's client to
	// connect here. Using graphite instead of influxdb should be more
	// flexible, at least initially.
	for i := 0; i < numCPU; i++ {
		carbon, err := graphite.NewGraphite(config.GraphiteAddr, config.GraphitePort)
		if err != nil {
			panic(err)
		}
		go processBuffer(bufCh, carbon, i)
	}
}

func main() {
	// First fire up a queue to consume metric def events
	mdConn, err := amqp.Dial(config.RabbitMQURL)
	if err != nil {
		logger.Criticalf(err.Error())
		os.Exit(1)
	}
	defer mdConn.Close()
	logger.Debugf("connected")

	done := make(chan error, 1)

	// create a publisher
	pub, err := qproc.CreatePublisher(mdConn, "metricEvents", "fanout")
	if err != nil {
		logger.Criticalf(err.Error())
		os.Exit(1)
	}

	var numCPU int
	if config.NumWorkers != 0 {
		numCPU = config.NumWorkers
	} else {
		numCPU = runtime.NumCPU()
	}

	err = qproc.ProcessQueue(mdConn, nil, "metrics", "topic", "metrics.*", "", done, processMetricDefEvent, numCPU)
	if err != nil {
		logger.Criticalf(err.Error())
		os.Exit(1)
	}
	err = qproc.ProcessQueue(mdConn, pub, "metricResults", "x-consistent-hash", "10", "", done, processMetrics, numCPU)
	if err != nil {
		logger.Criticalf(err.Error())
		os.Exit(1)
	}
	err = initEventProcessing(mdConn, numCPU, done)
	if err != nil {
		logger.Criticalf(err.Error())
		os.Exit(1)
	}

	// Signal handling. If SIGQUIT is received, print out the current
	// stack. Otherwise if SIGINT or SIGTERM are received clean up and exit
	// in an orderly fashion.
	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGQUIT, os.Interrupt, syscall.SIGTERM)
		buf := make([]byte, 1<<20)
		for sig := range sigs {
			if sig == syscall.SIGQUIT {
				// print out the current stack on SIGQUIT
				runtime.Stack(buf, true)
				log.Printf("=== received SIGQUIT ===\n*** goroutine dump...\n%s\n*** end\n", buf)
			} else {
				// finish our existing work, clean up, and exit
				// in an orderly fashion
				logger.Infof("Closing rabbitmq connection")
				cerr := mdConn.Close()
				if cerr != nil {
					logger.Errorf("Received error closing rabbitmq connection: %s", cerr.Error())
				}
				logger.Infof("Closing processing buffer channel")
				close(bufCh)
			}
		}
	}()

	// this channel returns when one of the workers exits.
	err = <-done
	logger.Criticalf("all done!")
	if err != nil {
		logger.Criticalf("Had an error, aiiieeee! '%s'", err.Error())
	}
}

func processMetrics(pub *qproc.Publisher, d *amqp.Delivery) error {
	metrics := make([]map[string]interface{}, 0)
	if err := json.Unmarshal(d.Body, &metrics); err != nil {
		return err
	}

	logger.Debugf("The parsed out json: %v", metrics)

	for _, m := range metrics {
		logger.Debugf("processing %s", m["name"])
		id := fmt.Sprintf("%d.%s", int64(m["org_id"].(float64)), m["name"])
		metricDefs.m.RLock()
		// Normally I would use defer unlock, but here we might need to
		// release the r/w lock and take an exclusive lock, so we have
		// to be more explicit about it.
		if md, ok := metricDefs.mdefs[id]; !ok {
			logger.Debugf("adding %s to metric defs", id)
			def, err := metricdef.GetMetricDefinition(id)
			if err != nil {
				if err.Error() == "record not found" {
					// create a new metric
					logger.Debugf("creating new metric")
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
			md.cache.raw.flushTime = now - int64(config.shortDuration)
			md.cache.aggr.flushTime = now - int64(config.longDuration)
			metricDefs.m.RUnlock()
			metricDefs.m.Lock()
			metricDefs.mdefs[id] = md
			metricDefs.m.Unlock()
		} else {
			metricDefs.m.RUnlock()
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
	logger.Debugf("Metric we have: %v :: %q", metric, metric)
	logger.Debugf("Updating metric def for %s", metric.ID)
	metricDefs.m.Lock()
	defer metricDefs.m.Unlock()

	md, ok := metricDefs.mdefs[metric.ID]
	md.m.RLock()
	defer md.m.RUnlock()
	newMd := &metricDef{mdef: metric}
	if ok {
		logger.Debugf("metric %s found", metric.ID)
		if md.mdef.LastUpdate >= metric.LastUpdate {
			logger.Debugf("%s already up to date", metric.ID)
			return nil
		}
		newMd.cache = md.cache
	} else {
		logger.Infof("no definition for %s found, building new cache", metric.ID)
		newMd.cache = buildMetricDefCache()
		now := time.Now().Unix()
		newMd.cache.raw.flushTime = now - int64(config.shortDuration)
		newMd.cache.aggr.flushTime = now - int64(config.longDuration)
	}
	metricDefs.mdefs[metric.ID] = newMd

	return nil
}

func removeMetricDef(metric *metricdef.MetricDefinition) error {
	logger.Debugf("Metric we have: %v :: %q", metric, metric)
	logger.Debugf("Removing metric def for %s", metric.ID)
	metricDefs.m.Lock()
	defer metricDefs.m.Unlock()
	md, ok := metricDefs.mdefs[metric.ID]
	if !ok {
		return nil
	}
	md.m.Lock()
	defer md.m.Unlock()

	delete(metricDefs.mdefs, metric.ID)
	return nil
}

func storeMetric(m map[string]interface{}, pub *qproc.Publisher) error {
	logger.Debugf("storing metric: %+v", m)
	met, err := buildIndvMetric(m)
	if err != nil {
		return err
	}
	if met.valReal {
		logger.Debugf("Adding to buffer")
		b := graphite.NewMetric(met.id, strconv.FormatFloat(met.value, 'f', -1, 64), met.time)
		bufCh <- b
	}
	rollupRaw(met)
	checkThresholds(met, pub)
	return nil
}

func processBuffer(c <-chan graphite.Metric, carbon *graphite.Graphite, workerID int) {
	buf := make([]graphite.Metric, 0)

	// flush buffer every second
	t := time.NewTicker(time.Second)
	for {
		select {
		case b := <-c:
			if b.Name != "" {
				logger.Debugf("worker %d appending to buffer", workerID)
				buf = append(buf, b)
			}
		case <-t.C:
			// A possibility: it might be worth it to hack up the
			// carbon lib to allow batch submissions of metrics if
			// doing them individually proves to be too slow
			logger.Debugf("worker %d flushing %d items in buffer now", workerID, len(buf))
			for _, m := range buf {
				logger.Debugf("worker %d sending metric %+v", workerID, m)
				err := carbon.SendMetric(m)
				if err != nil {
					logger.Errorf(err.Error())
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

	logger.Debugf("rolling up %s", met.id)

	if def.cache.raw.flushTime < (met.time - int64(config.shortDuration)) {
		if def.cache.aggr.flushTime < (met.time - int64(config.longDuration)) {
			logger.Debugf("rolling up 6 hour for %s", met.id)
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
			rollupTime := durationStr(config.longDuration)
			if avg != nil {
				logger.Debugf("writing %s rollup for %s", rollupTime, met.id)
				id := fmt.Sprintf("%s.avg.%s", rollupTime, met.id)
				b := graphite.NewMetric(id, strconv.FormatFloat(*avg, 'f', -1, 64), met.time)
				bufCh <- b
			}
			if min != nil {
				id := fmt.Sprintf("%s.min.%s", rollupTime, met.id)
				b := graphite.NewMetric(id, strconv.FormatFloat(*min, 'f', -1, 64), met.time)
				bufCh <- b
			}
			if max != nil {
				id := fmt.Sprintf("%s.max.%s", rollupTime, met.id)
				b := graphite.NewMetric(id, strconv.FormatFloat(*max, 'f', -1, 64), met.time)
				bufCh <- b
			}
		}
		rollupTime := durationStr(config.shortDuration)
		logger.Debugf("rolling up %s for %s", rollupTime, met.id)
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
		def.cache.raw.data = nil
		def.cache.raw.flushTime = met.time
		if avg != nil {
			logger.Debugf("writing %s rollup for %s:%f", rollupTime, met.id, *avg)
			id := fmt.Sprintf("%s.avg.%s", rollupTime, met.id)
			b := graphite.NewMetric(id, strconv.FormatFloat(*avg, 'f', -1, 64), met.time)
			bufCh <- b
		}
		if min != nil {
			id := fmt.Sprintf("%s.min.%s", rollupTime, met.id)
			b := graphite.NewMetric(id, strconv.FormatFloat(*min, 'f', -1, 64), met.time)
			bufCh <- b
		}
		if max != nil {
			id := fmt.Sprintf("%s.max.%s", rollupTime, met.id)
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
	d := metricDefs.mdefs[met.id]
	d.m.Lock()
	defer d.m.Unlock()
	def := d.mdef

	state := stateOK
	var msg string
	thresholds := def.Thresholds

	if thresholds.CritMin != nil && met.value < thresholds.CritMin.(float64) {
		msg = fmt.Sprintf("%f less than criticalMin %f", met.value, thresholds.CritMin.(float64))
		logger.Debugf(msg)
		state = stateCrit
	}
	if state < stateCrit && thresholds.CritMax != nil && met.value > thresholds.CritMax.(float64) {
		msg = fmt.Sprintf("%f greater than criticalMax %f", met.value, thresholds.CritMax.(float64))
		logger.Debugf(msg)
		state = stateCrit
	}
	if state < stateWarn && thresholds.WarnMin != nil && met.value < thresholds.WarnMin.(float64) {
		msg = fmt.Sprintf("%f less than warnMin %f", met.value, thresholds.WarnMin.(float64))
		logger.Debugf(msg)
		state = stateWarn
	}
	if state < stateWarn && thresholds.WarnMax != nil && met.value < thresholds.WarnMax.(float64) {
		msg = fmt.Sprintf("%f greater than warnMax %f", met.value, thresholds.WarnMax.(float64))
		logger.Debugf(msg)
		state = stateWarn
	}

	levelMap := []string{"ok", "warning", "critical"}
	var updates bool
	events := make([]map[string]interface{}, 0)
	curState := def.State
	if state != def.State {
		logger.Infof("state has changed for %s. Was '%s', now '%s'", def.ID, levelMap[state], levelMap[def.State])
		def.State = state
		// TODO: ask why in the node version lastUpdate is set with
		// 'new Date(metric.time * 1000).getTime()'
		def.LastUpdate = time.Now().Unix()
		updates = true
	} else if def.KeepAlives != 0 && (def.LastUpdate < (met.time - int64(def.KeepAlives))) {
		logger.Debugf("No updates in %d seconds, sending keepAlive", def.KeepAlives)
		updates = true
		def.LastUpdate = time.Now().Unix()
		checkEvent := map[string]interface{}{"source": "metric", "metric": met.name, "org_id": met.orgID, "type": "keepAlive", "state": levelMap[state], "details": msg, "timestamp": met.time * 1000}
		events = append(events, checkEvent)
	}
	if updates {
		err := def.Save()
		if err != nil {
			logger.Errorf("Error updating metric definition for %s: %s", def.ID, err.Error())
			delete(metricDefs.mdefs, def.ID)
			return
		}
		logger.Debugf("%s update committed to elasticsearch", def.ID)
	}
	if state > stateOK {
		checkEvent := map[string]interface{}{"source": "metric", "metric": met.name, "org_id": met.orgID, "type": "checkFailure", "state": levelMap[state], "details": msg, "timestamp": met.time * 1000}
		events = append(events, checkEvent)
	}
	if state != curState {
		metricEvent := map[string]interface{}{"source": "metric", "metric": met.name, "org_id": met.orgID, "type": "stateChange", "state": levelMap[state], "details": fmt.Sprintf("state transitioned from %s to %s", levelMap[curState], levelMap[state]), "timestamp": met.time * 1000}
		events = append(events, metricEvent)
	}
	if len(events) > 0 {
		logger.Infof("publishing events")
		for _, e := range events {
			err := pub.PublishMsg(e["type"].(string), e)
			if err != nil {
				logger.Errorf("Failed to publish event: %s", err.Error())
			}
		}
	}
}

func buildIndvMetric(m map[string]interface{}) (*indvMetric, error) {
	var valReal bool
	var val float64

	if v, exists := m["value"]; exists {
		if vf, ok := v.(float64); ok {
			val = vf
			valReal = true
		}
	}

	// validate input
	strs := [...]string{"name", "metric", "location", "unit", "target_type"}
	floats := [...]string{"org_id", "interval", "time", "site_id", "monitor_id"}

	for _, s := range strs {
		if _, ok := m[s].(string); !ok && m[s] != nil {
			return nil, fmt.Errorf("%s is not a string", s)
		}
	}
	for _, f := range floats {
		if _, ok := m[f].(float64); !ok && m[f] != nil {
			return nil, fmt.Errorf("%s is not a number", f)
		}
	}
	id := fmt.Sprintf("%d.%s", int64(m["org_id"].(float64)), m["name"])

	met := &indvMetric{id: id,
		orgID:      int(m["org_id"].(float64)),
		name:       m["name"].(string),
		metric:     m["metric"].(string),
		location:   m["location"].(string),
		interval:   int(m["interval"].(float64)),
		value:      val,
		valReal:    valReal,
		unit:       m["unit"].(string),
		time:       int64(math.Floor(m["time"].(float64))),
		siteID:     int(m["site_id"].(float64)),
		monitorID:  int(m["monitor_id"].(float64)),
		targetType: m["target_type"].(string)}
	return met, nil
}

func durationStr(d time.Duration) string {
	if d.Hours() >= 1 {
		return fmt.Sprintf("%dhour", int(d.Hours()))
	} else if d.Minutes() >= 1 {
		return fmt.Sprintf("%dmin", int(d.Minutes()))
	} else {
		return fmt.Sprintf("%dseconds", int(d.Seconds()))
	}
}
