package main

import (
	"github.com/raintank/raintank-metric/eventdef"
	"github.com/raintank/raintank-metric/qproc"
	"github.com/streadway/amqp"
)

func initEventProcessing(mdConn *amqp.Connection, errCh chan error) error {
	err := qproc.ProcessQueue(mdConn, nil, "grafana_events", "topic", "EVENT.#", "", errCh, processEvent)
	if err != nil {
		return err
	}
	return nil
}

func processEvent(pub *qproc.Publisher, d *amqp.Delivery) error {
	event, err := eventdef.EventFromJSON(d.Body)
	if err != nil {
		return err
	}
	if err = event.Save(); err != nil {
		return err
	}
	if err := d.Ack(false); err != nil {
		return err
	}
	return nil
}
