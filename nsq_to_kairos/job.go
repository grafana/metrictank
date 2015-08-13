package main

import "github.com/bitly/go-nsq"

type Job struct {
	msg  *nsq.Message
	done chan error
}

func NewJob(msg *nsq.Message) Job {
	return Job{
		msg,
		make(chan error),
	}
}
