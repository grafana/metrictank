package main

import (
	"bytes"
	"encoding/binary"
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
}

func NewKairosGateway(dryRun bool) (*KairosGateway, error) {
	kairos, err := metricstore.NewKairosdb("http://kairosdb:8080")
	if err != nil {
		return nil, err
	}
	kg := &KairosGateway{
		kairos:     kairos,
		inHighPrio: make(chan Job, 4), // always useful to have some packets queue up to enforce the prio
		inLowPrio:  make(chan Job),
		dryRun:     dryRun,
	}
	go kg.Run()
	return kg, nil
}

func (kg *KairosGateway) Run() {
	for {
		select {
		case job := <-kg.inHighPrio:
			job.done <- kg.process("high-prio", job.msg)
		default:
			select {
			case job := <-kg.inHighPrio:
				job.done <- kg.process("high-prio", job.msg)
			case job := <-kg.inLowPrio:
				job.done <- kg.process("low--prio", job.msg)
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
	format := "unknown"
	if msg.Body[0] == '\x00' {
		format = "msgFormatMetricDefinitionArrayJson"
	}
	log.Printf("DEBUG: processing %s msg %s. timestamp: %s. format: %s. attempts: %d\n", qualifier, msg.ID, time.Unix(0, msg.Timestamp), format, msg.Attempts)
	buf := bytes.NewReader(msg.Body[1:9])
	var id int64
	binary.Read(buf, binary.BigEndian, &id)
	log.Printf("DEBUG: DIETER-PROCESS-%s %d\n", qualifier, id)
	metrics := make([]*metricdef.IndvMetric, 0)
	if err := json.Unmarshal(msg.Body[9:], &metrics); err != nil {
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
	log.Printf("DEBUG: DIETER-FINISHED-%s %d\n", qualifier, id)
	return err
}
