package main

import (
	"log"
	"time"

	"github.com/bitly/go-hostpool"
	"github.com/bitly/go-nsq"
)

type KairosHandler struct {
	gateway   *KairosGateway
	producers map[string]*nsq.Producer
	hostPool  hostpool.HostPool
}

func NewKairosHandler(gateway *KairosGateway, hostPool hostpool.HostPool, producers map[string]*nsq.Producer) *KairosHandler {
	return &KairosHandler{
		gateway:   gateway,
		hostPool:  hostPool,
		producers: producers,
	}
}
func (k *KairosHandler) trySubmit(body []byte) error {
	hostPoolResponse := k.hostPool.Get()
	p := k.producers[hostPoolResponse.Host()]
	err := p.Publish(*topicLowPrio, body)
	if err != nil {
		log.Printf("WARN : publisher marking host %s as faulty due to %s", hostPoolResponse.Host(), err)
		hostPoolResponse.Mark(err)
	}
	return err
}

func (k *KairosHandler) HandleMessage(m *nsq.Message) error {
	created := time.Unix(0, m.Timestamp)
	if time.Now().Add(-time.Duration(4) * time.Minute).After(created) {
		log.Printf("DEBUG: requeuing msg %s. timestamp: %s. attempts: %d\n ", m.ID, time.Unix(0, m.Timestamp), m.Attempts)
		attempts := 3 // try 3 different hosts before giving up and requeuing
		var err error
		for attempt := 1; attempt <= attempts; attempt++ {
			err = k.trySubmit(m.Body)
			if err == nil {
				return nil // we published the msg as lowprio and can mark it as processed
			}
		}
		log.Println("WARN : failed to publish out of date message as low-prio. reprocessing later")
		return err
	}
	return k.gateway.ProcessHighPrio(m)
}
