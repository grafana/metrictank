package main

import (
	"time"

	"github.com/nsqio/go-nsq"
	"github.com/grafana/grafana/pkg/log"
	"github.com/raintank/raintank-metric/metricstore"
)

type KairosGateway struct {
	kairos     *metricstore.Kairosdb
	inHighPrio chan Job
	inLowPrio  chan Job
	dryRun     bool
	workers    int
}

func NewKairosGateway(addr string, dryRun bool, workers int) (*KairosGateway, error) {
	kairos, err := metricstore.NewKairosdb(addr)
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
			inHighPrioItems.Value(int64(len(kg.inHighPrio)))
		default:
			select {
			case job = <-kg.inHighPrio:
				inHighPrioItems.Value(int64(len(kg.inHighPrio)))
			case job = <-kg.inLowPrio:
				inLowPrioItems.Value(int64(len(kg.inLowPrio)))
			}
		}
		job.done <- kg.process(job)
	}
}
func (kg *KairosGateway) ProcessHighPrio(msg *nsq.Message) error {
	job, err := NewJob(msg, "high-prio")
	if err != nil {
		return err
	}
	inHighPrioItems.Value(int64(len(kg.inHighPrio)))
	msgsHighPrioAge.Value(time.Now().Sub(job.Msg.Produced).Nanoseconds() / 1000)
	kg.inHighPrio <- job
	return <-job.done
}
func (kg *KairosGateway) ProcessLowPrio(msg *nsq.Message) error {
	job, err := NewJob(msg, "low-prio")
	if err != nil {
		return err
	}
	inLowPrioItems.Value(int64(len(kg.inLowPrio)))
	msgsLowPrioAge.Value(time.Now().Sub(job.Msg.Produced).Nanoseconds() / 1000)
	kg.inLowPrio <- job
	return <-job.done
}

// error is what is used to determine to ACK or NACK
func (kg *KairosGateway) process(job Job) error {
	msg := job.msg
	messagesSize.Value(int64(len(job.Msg.Msg)))
	log.Debug("processing metrics %s %d. timestamp: %s. format: %s. attempts: %d\n", job.qualifier, job.Msg.Id, time.Unix(0, msg.Timestamp), job.Msg.Format, msg.Attempts)

	err := job.Msg.DecodeMetricData()
	if err != nil {
		log.Info(err, "skipping message")
		return nil
	}

	metricsPerMessage.Value(int64(len(job.Msg.Metrics)))
	if !kg.dryRun {
		pre := time.Now()
		err = kg.kairos.SendMetricPointers(job.Msg.Metrics)
		if err != nil {
			metricsToKairosFail.Inc(int64(len(job.Msg.Metrics)))
			log.Warn("can't send to kairosdb: %s. retrying later", err)
		} else {
			metricsToKairosOK.Inc(int64(len(job.Msg.Metrics)))
			kairosPutDuration.Value(time.Now().Sub(pre))
		}
	}
	log.Debug("finished metrics %s %d - %d metrics sent\n", job.qualifier, job.Msg.Id, len(job.Msg.Metrics))
	return err
}
