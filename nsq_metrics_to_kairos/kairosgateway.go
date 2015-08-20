package main

import (
	"encoding/json"
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
	dryRun     bool
	workers    int
}

func NewKairosGateway(dryRun bool, workers int) (*KairosGateway, error) {
	kairos, err := metricstore.NewKairosdb("http://kairosdb:8080")
	if err != nil {
		return nil, err
	}
	// re in*Prio chan sizes:
	// we should make sure we can sufficiently pre-fill the queue so that
	// the priorities are properly honored when the workers all once collect new jobs (and potentially finish 'em fairly fast.
	// on the other hand, there's also no point in pulling in much more messages if we can't handle them.
	kg := &KairosGateway{
		kairos:     kairos,
		inHighPrio: make(chan Job, workers*2),
		inLowPrio:  make(chan Job, workers*2),
		dryRun:     dryRun,
		workers:    workers,
	}
	for i := 1; i <= workers; i++ {
		go kg.work()
	}
	return kg, nil
}

// work() always prefers high prio jobs, when available
func (kg *KairosGateway) work() {
	var job Job
	for {
		select {
		case job = <-kg.inHighPrio:
		default:
			select {
			case job = <-kg.inHighPrio:
			case job = <-kg.inLowPrio:
			}
		}
		job.done <- kg.process(job)
	}
}
func (kg *KairosGateway) ProcessHighPrio(msg *nsq.Message) error {
	job := NewJob(msg, "high-prio")
	kg.inHighPrio <- job
	return <-job.done
}
func (kg *KairosGateway) ProcessLowPrio(msg *nsq.Message) error {
	job := NewJob(msg, "low-prio")
	kg.inLowPrio <- job
	return <-job.done
}

func (kg *KairosGateway) process(job Job) error {
	msg := job.msg
	log.Printf("DEBUG: processing metrics %s %d. timestamp: %s. format: %s. attempts: %d\n", job.qualifier, job.id, time.Unix(0, msg.Timestamp), job.format, msg.Attempts)
	metrics := make([]*metricdef.IndvMetric, 0)
	if err := json.Unmarshal(job.Body, &metrics); err != nil {
		log.Printf("ERROR: failure to unmarshal message body: %s. skipping message", err)
		return nil
	}
	var err error
	if !kg.dryRun {
		err = kg.kairos.SendMetricPointers(metrics)
		if err != nil {
			log.Printf("WARNING: can't send to kairosdb: %s. retrying later", err)
		}
	}
	log.Printf("DEBUG: finished metrics %s %d - %d metrics sent\n", job.qualifier, job.id, len(metrics))
	return err
}
