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
	"github.com/raintank/raintank-metric/qproc"
	"github.com/raintank/raintank-metric/metricdef"
	"github.com/streadway/amqp"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

type publisher struct {
	*amqp.Channel
}

type metricDefCache struct {
	mdefs map[string]*metric
	m sync.RWMutex
}

// Fill this out once it's clear what should be in here
type metric struct {
	mdef *metricdef.MetricDefinition
	cache *metricCache
}

type metricCache struct {
	raw *cacheRaw
	aggr *cacheAggr
}

type cacheRaw struct {
	data []string
	flushTime time.Time
}

type cacheAggr struct {
	data *aggrData
	flushTime time.Time
}

type aggrData struct {
	avg []int
	min []int
	max []int
}

var metricDefs *metricDefCache

type PayloadProcessor func(*publisher, *amqp.Delivery) error

// dev var declarations, until real config/flags are added
var rabbitURL string = "amqp://rabbitmq"

func init() {
	metricDefs = &metricDefCache{}
	metricDefs.mdefs = make(map[string]*metric)
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

	for _, metric := range metrics {
		fmt.Printf("would process %s\n", metric["name"])
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
		payload := make(map[string]interface{})
		if err := json.Unmarshal(d.Body, &payload); err != nil {
			return err
		}
		if err := updateMetricDef(payload); err != nil {
			return err
		}
	case "remove":
		payload := make(map[string]interface{})
		if err := json.Unmarshal(d.Body, &payload); err != nil {
			return err
		}
		if err := removeMetricDef(payload); err != nil {
			return err
		}
	default:
		err := fmt.Errorf("message has unknown action '%s'", action)
		return err
	}

	return nil
}

func updateMetricDef(payload map[string]interface{}) error {
	fmt.Printf("The parsed out json: %v", payload)
	return nil
}

func removeMetricDef(payload map[string]interface{}) error {
	fmt.Printf("The parsed out json: %v", payload)
	return nil
}
