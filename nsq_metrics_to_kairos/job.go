package main

import (
	"bytes"
	"encoding/binary"

	"github.com/bitly/go-nsq"
)

type Job struct {
	msg       *nsq.Message
	done      chan error
	qualifier string
	format    string
	id        int64
	Body      []byte
}

func NewJob(msg *nsq.Message, qualifier string) Job {
	job := Job{
		msg,
		make(chan error),
		qualifier,
		"unknown",
		0,
		msg.Body[9:],
	}
	if msg.Body[0] == '\x00' {
		job.format = "msgFormatMetricDefinitionArrayJson"
	}
	buf := bytes.NewReader(msg.Body[1:9])
	binary.Read(buf, binary.BigEndian, &job.id)
	return job
}
