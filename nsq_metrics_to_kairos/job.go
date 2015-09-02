package main

import (
	"bytes"
	"encoding/binary"
	"time"

	"github.com/nsqio/go-nsq"
)

type Job struct {
	msg       *nsq.Message
	done      chan error
	qualifier string
	format    string
	id        int64 // ID from message, should be a unique. also contains Produced time
	Body      []byte
	Produced  time.Time // when the job was created by the producer
}

func NewJob(msg *nsq.Message, qualifier string) Job {
	job := Job{
		msg,
		make(chan error),
		qualifier,
		"unknown",
		0,
		msg.Body[9:],
		time.Time{},
	}
	if msg.Body[0] == '\x00' {
		job.format = "msgFormatMetricDefinitionArrayJson"
	}
	if msg.Body[0] == '\x01' {
		job.format = "msgFormatMetricDataArrayMsgp"
	}

	buf := bytes.NewReader(msg.Body[1:9])
	binary.Read(buf, binary.BigEndian, &job.id)
	job.Produced = time.Unix(0, job.id)
	return job
}
