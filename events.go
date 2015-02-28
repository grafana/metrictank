/*
 * Copyright (c) 2015, Raintank Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package main

import (
	"github.com/raintank/raintank-metric/eventdef"
	"github.com/raintank/raintank-metric/qproc"
	"github.com/streadway/amqp"
)

func initEventProcessing(mdConn *amqp.Connection, numWorkers int, errCh chan error) error {
	err := qproc.ProcessQueue(mdConn, nil, "grafana_events", "topic", "EVENT.#", "", errCh, processEvent, numWorkers)
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
