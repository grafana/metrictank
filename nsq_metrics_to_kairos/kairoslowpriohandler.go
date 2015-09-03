package main

import "github.com/nsqio/go-nsq"

type KairosLowPrioHandler struct {
	gateway *KairosGateway
}

func NewKairosLowPrioHandler(gateway *KairosGateway) *KairosLowPrioHandler {
	return &KairosLowPrioHandler{
		gateway: gateway,
	}
}

func (k *KairosLowPrioHandler) HandleMessage(m *nsq.Message) error {
	err := k.gateway.ProcessLowPrio(m)
	if err != nil {
		msgsHandleLowPrioFail.Inc(1)
	} else {
		msgsHandleLowPrioOK.Inc(1)
	}
	return err
}
