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
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
)

var metricDefs *metricdef.MetricDefCache

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
	if err != nil {
		panic(err)
	}

	metricDefs, err = metricdef.InitMetricDefCache(config.shortDuration, config.longDuration, config.RedisAddr, config.RedisPasswd, config.RedisDB)
	if err != nil {
		panic(err)
	}

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
	metrics := make([]*metricdef.IndvMetric, 0)
	if err := json.Unmarshal(d.Body, &metrics); err != nil {
		return err
	}

	logger.Debugf("The parsed out json: %v", metrics)

	for _, m := range metrics {
		logger.Debugf("processing %s", m.Name)
		id := fmt.Sprintf("%d.%s", m.OrgId, m.Name)
		if m.Id == "" {
			m.Id = id
		}
		if err := metricDefs.CheckMetricDef(id, m); err != nil {
			return err
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
		if err := metricDefs.UpdateDefCache(metric); err != nil {
			return err
		}
	case "remove":
		metric, err := metricdef.DefFromJSON(d.Body)
		if err != nil {
			return err
		}
		metricDefs.RemoveDefCache(metric.Id)
	default:
		err := fmt.Errorf("message has unknown action '%s'", action)
		return err
	}

	return nil
}

func storeMetric(met *metricdef.IndvMetric, pub *qproc.Publisher) error {
	logger.Debugf("storing metric: %+v", met)
	b := graphite.NewMetric(met.Id, strconv.FormatFloat(met.Value, 'f', -1, 64), met.Time)
	bufCh <- b
	go func(met *metricdef.IndvMetric, pub *qproc.Publisher) {
		// align to a minute boundary without holding everything up
		rollupRaw(met)
		checkThresholds(met, pub)
	}(met, pub)
	return nil
}

func processBuffer(c <-chan graphite.Metric, carbon *graphite.Graphite, workerId int) {
	buf := make([]graphite.Metric, 0)

	// flush buffer every second
	t := time.NewTicker(time.Second)
	for {
		select {
		case b := <-c:
			if b.Name != "" {
				logger.Debugf("worker %d appending to buffer", workerId)
				buf = append(buf, b)
			}
		case <-t.C:
			// A possibility: it might be worth it to hack up the
			// carbon lib to allow batch submissions of metrics if
			// doing them individually proves to be too slow
			logger.Debugf("worker %d flushing %d items in buffer now", workerId, len(buf))
			for _, m := range buf {
				logger.Debugf("worker %d sending metric %+v", workerId, m)
				err := carbon.SendMetric(m)
				if err != nil {
					logger.Errorf(err.Error())
				}
			}
			buf = nil
		}
	}
}

func rollupRaw(met *metricdef.IndvMetric) {
	def, err := metricDefs.GetDefItem(met.Id)
	if err != nil {
		logger.Errorf(err.Error())
		return
	}
	def.Lock()
	defer def.Unlock()

	logger.Debugf("rolling up %s", met.Id)

	if def.Cache.Raw.FlushTime < (met.Time - int64(config.shortDuration/time.Second)) {
		if def.Cache.Aggr.FlushTime < (met.Time - int64(config.longDuration/time.Second)) {
			logger.Debugf("rolling up 6 hour for %s", met.Id)
			var min, max, avg, sum *float64
			count := len(def.Cache.Aggr.Data.Min)
			// not slavish; we need to manipulate three slices at
			// once
			for i := 0; i < count; i++ {
				if min == nil || *def.Cache.Aggr.Data.Min[i] < *min {
					min = def.Cache.Aggr.Data.Min[i]
				}
				if max == nil || *def.Cache.Aggr.Data.Max[i] < *max {
					max = def.Cache.Aggr.Data.Max[i]
				}
				if def.Cache.Aggr.Data.Avg[i] != nil {
					if sum == nil {
						sum = def.Cache.Aggr.Data.Avg[i]
					} else {
						*sum += *def.Cache.Aggr.Data.Avg[i]
					}
				}
			}
			if count > 0 {
				z := *sum / float64(count)
				avg = &z
			}
			def.Cache.Aggr.Data.Avg = nil
			def.Cache.Aggr.Data.Min = nil
			def.Cache.Aggr.Data.Max = nil
			def.Cache.Aggr.FlushTime = met.Time
			rollupTime := durationStr(config.longDuration)
			if avg != nil {
				logger.Debugf("writing %s rollup for %s", rollupTime, met.Id)
				id := fmt.Sprintf("%s.avg.%s", rollupTime, met.Id)
				b := graphite.NewMetric(id, strconv.FormatFloat(*avg, 'f', -1, 64), met.Time)
				bufCh <- b
			}
			if min != nil {
				id := fmt.Sprintf("%s.min.%s", rollupTime, met.Id)
				b := graphite.NewMetric(id, strconv.FormatFloat(*min, 'f', -1, 64), met.Time)
				bufCh <- b
			}
			if max != nil {
				id := fmt.Sprintf("%s.max.%s", rollupTime, met.Id)
				b := graphite.NewMetric(id, strconv.FormatFloat(*max, 'f', -1, 64), met.Time)
				bufCh <- b
			}
		}
		rollupTime := durationStr(config.shortDuration)
		logger.Debugf("rolling up %s for %s", rollupTime, met.Id)
		var min, max, avg, sum *float64
		count := len(def.Cache.Raw.Data)
		for _, p := range def.Cache.Raw.Data {
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
		def.Cache.Raw.Data = nil
		def.Cache.Raw.FlushTime = met.Time
		if avg != nil {
			logger.Debugf("writing %s rollup for %s:%f", rollupTime, met.Id, *avg)
			id := fmt.Sprintf("%s.avg.%s", rollupTime, met.Id)
			b := graphite.NewMetric(id, strconv.FormatFloat(*avg, 'f', -1, 64), met.Time)
			bufCh <- b
		}
		if min != nil {
			id := fmt.Sprintf("%s.min.%s", rollupTime, met.Id)
			b := graphite.NewMetric(id, strconv.FormatFloat(*min, 'f', -1, 64), met.Time)
			bufCh <- b
		}
		if max != nil {
			id := fmt.Sprintf("%s.max.%s", rollupTime, met.Id)
			b := graphite.NewMetric(id, strconv.FormatFloat(*max, 'f', -1, 64), met.Time)
			bufCh <- b
		}
		def.Cache.Aggr.Data.Min = append(def.Cache.Aggr.Data.Min, min)
		def.Cache.Aggr.Data.Max = append(def.Cache.Aggr.Data.Max, max)
		def.Cache.Aggr.Data.Avg = append(def.Cache.Aggr.Data.Avg, avg)
	}
	def.Cache.Raw.Data = append(def.Cache.Raw.Data, met.Value)
	if err = def.Save(); err != nil {
		logger.Errorf(err.Error())
	}
}

func checkThresholds(met *metricdef.IndvMetric, pub *qproc.Publisher) {
	d, err := metricDefs.GetDefItem(met.Id)
	if err != nil {
		logger.Errorf(err.Error())
		return
	}
	d.Lock()
	defer d.Unlock()
	def := d.Def

	state := metricdef.StateOK
	var msg string
	thresholds := def.Thresholds

	if thresholds.CritMin != nil && met.Value < thresholds.CritMin.(float64) {
		msg = fmt.Sprintf("%f less than criticalMin %f", met.Value, thresholds.CritMin.(float64))
		logger.Debugf(msg)
		state = metricdef.StateCrit
	}
	if state < metricdef.StateCrit && thresholds.CritMax != nil && met.Value > thresholds.CritMax.(float64) {
		msg = fmt.Sprintf("%f greater than criticalMax %f", met.Value, thresholds.CritMax.(float64))
		logger.Debugf(msg)
		state = metricdef.StateCrit
	}
	if state < metricdef.StateWarn && thresholds.WarnMin != nil && met.Value < thresholds.WarnMin.(float64) {
		msg = fmt.Sprintf("%f less than warnMin %f", met.Value, thresholds.WarnMin.(float64))
		logger.Debugf(msg)
		state = metricdef.StateWarn
	}
	if state < metricdef.StateWarn && thresholds.WarnMax != nil && met.Value < thresholds.WarnMax.(float64) {
		msg = fmt.Sprintf("%f greater than warnMax %f", met.Value, thresholds.WarnMax.(float64))
		logger.Debugf(msg)
		state = metricdef.StateWarn
	}

	var updates bool
	events := make([]map[string]interface{}, 0)
	curState := def.State
	if state != def.State {
		logger.Infof("state has changed for %s. Was '%s', now '%s'", def.Id, metricdef.LevelMap[state], metricdef.LevelMap[def.State])
		def.State = state
		// TODO: ask why in the node version lastUpdate is set with
		// 'new Date(metric.time * 1000).getTime()'
		def.LastUpdate = time.Now().Unix()
		updates = true
	} else if def.KeepAlives != 0 && (def.LastUpdate < (met.Time - int64(def.KeepAlives))) {
		logger.Debugf("No updates in %d seconds, sending keepAlive", def.KeepAlives)
		updates = true
		def.LastUpdate = time.Now().Unix()
		checkEvent := map[string]interface{}{"source": "metric", "metric": met.Name, "org_id": met.OrgId, "type": "keepAlive", "state": metricdef.LevelMap[state], "details": msg, "timestamp": met.Time * 1000}
		events = append(events, checkEvent)
	}
	if updates {
		err := def.Save()
		if err != nil {
			logger.Errorf("Error updating metric definition for %s: %s", def.Id, err.Error())
			metricDefs.RemoveDefFromMap(met.Id)
			return
		}
		logger.Debugf("%s update committed to elasticsearch", def.Id)
	}
	if state > metricdef.StateOK {
		checkEvent := map[string]interface{}{"source": "metric", "metric": met.Name, "org_id": met.OrgId, "type": "checkFailure", "state": metricdef.LevelMap[state], "details": msg, "timestamp": met.Time * 1000}
		events = append(events, checkEvent)
	}
	if state != curState {
		metricEvent := map[string]interface{}{"source": "metric", "metric": met.Name, "org_id": met.OrgId, "type": "stateChange", "state": metricdef.LevelMap[state], "details": fmt.Sprintf("state transitioned from %s to %s", metricdef.LevelMap[curState], metricdef.LevelMap[state]), "timestamp": met.Time * 1000}
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

func durationStr(d time.Duration) string {
	if d.Hours() >= 1 {
		return fmt.Sprintf("%dhour", int(d.Hours()))
	} else if d.Minutes() >= 1 {
		return fmt.Sprintf("%dmin", int(d.Minutes()))
	} else {
		return fmt.Sprintf("%dsecond", int(d.Seconds()))
	}
}
