package main

import (
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/bitly/go-nsq"
	"github.com/raintank/raintank-metric/metricdef"
	"github.com/raintank/raintank-metric/metricstore"
)

type KairosGateway struct {
	kairos     *metricstore.Kairosdb
	inHighPrio chan Job
	inLowPrio  chan Job
}

func NewKairosGateway() (*KairosGateway, error) {
	kairos, err := metricstore.NewKairosdb("http://kairosdb:8080")
	if err != nil {
		return nil, err
	}
	kg := &KairosGateway{
		kairos:     kairos,
		inHighPrio: make(chan Job, 4), // always useful to have some packets queue up to enforce the prio
		inLowPrio:  make(chan Job),
	}
	go kg.Run()
	return kg, nil
}

func (kg *KairosGateway) Run() {
	for {
		select {
		case job := <-kg.inHighPrio:
			job.done <- kg.process("high prio", job.msg)
		default:
			select {
			case job := <-kg.inHighPrio:
				job.done <- kg.process("high prio", job.msg)
			case job := <-kg.inLowPrio:
				job.done <- kg.process("low prio", job.msg)
			}
		}
	}
}
func (kg *KairosGateway) ProcessHighPrio(msg *nsq.Message) error {
	job := NewJob(msg)
	kg.inHighPrio <- job
	return <-job.done
}
func (kg *KairosGateway) ProcessLowPrio(msg *nsq.Message) error {
	job := NewJob(msg)
	kg.inLowPrio <- job
	return <-job.done
}

func (kg *KairosGateway) process(qualifier string, msg *nsq.Message) error {
	fmt.Printf("processing %s msg. timestamp: %s. attempts: %d\n", qualifier, time.Unix(0, msg.Timestamp), msg.Attempts)
	metrics := make([]*metricdef.IndvMetric, 0)
	if err := json.Unmarshal(msg.Body, &metrics); err != nil {
		log.Printf("ERROR: failure to unmarshal message body: %s. skipping message", err)
		return nil
	}
	err := kg.kairos.SendMetricPointers(metrics)
	if err != nil {
		log.Printf("ERROR: can't send to kairosdb: %s. retrying later", err)
	}
	return err
}
