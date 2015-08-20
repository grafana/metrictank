package main

import "github.com/bitly/go-nsq"

type KairosLowPrioHandler struct {
	gateway *KairosGateway
}

func NewKairosLowPrioHandler(gateway *KairosGateway) *KairosLowPrioHandler {
	return &KairosLowPrioHandler{
		gateway: gateway,
	}
}

func (k *KairosLowPrioHandler) HandleMessage(m *nsq.Message) error {
	return k.gateway.ProcessLowPrio(m)
}
