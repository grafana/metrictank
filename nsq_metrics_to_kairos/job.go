package main

import (
	"github.com/nsqio/go-nsq"
	"github.com/raintank/raintank-metric/msg"
)

type Job struct {
	msg       *nsq.Message
	done      chan error
	qualifier string
	Msg       msg.MetricData
}

func NewJob(m *nsq.Message, qualifier string) (Job, error) {
	ms, err := msg.MetricDataFromMsg(m.Body)
	if err != nil {
		return Job{}, err
	}
	job := Job{
		m,
		make(chan error),
		qualifier,
		ms,
	}
	return job, nil
}
