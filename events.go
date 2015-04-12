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
 	"github.com/ctdk/goas/v2/logger"
	"github.com/raintank/raintank-metric/eventdef"
	"github.com/raintank/raintank-metric/qproc"
	"github.com/streadway/amqp"
	"net/http"
	"encoding/json"
	"time"
	"bytes"
	"io/ioutil"
)

func initEventProcessing(mdConn *amqp.Connection, numWorkers int, errCh chan error) error {
	err := qproc.ProcessQueue(mdConn, "grafana_events", "topic", "eventQueue", "EVENT.#", true, false, false, errCh, processEvent, numWorkers)
	if err != nil {
		return err
	}
	return nil
}

func processEvent(d *amqp.Delivery) error {
	event, err := eventdef.EventFromJSON(d.Body)
	if err != nil {
		return err
	}
	if event.EventType == "monitor_state" {
		if err := updateState(event); err != nil {
			return err
		}
	}
	if err = event.Save(); err != nil {
		return err
	}
	if err := d.Ack(false); err != nil {
		return err
	}
	return nil
}

type StateChange struct {
	EndpointId float64 `json:"endpoint_id"`
	OrgId  int64 `json:"org_id"`
	MonitorId float64 `json:"monitor_id"`
	CollectorId  float64 `json:"collector_id"`
	Updated    time.Time `json:"updated"`
	State  int64 `json:"state"`
}

func updateState(event *eventdef.EventDefinition) error {
	collector, ok := event.Extra["collector_id"]
	if !ok {
		logger.Errorf("event does not have collector_id set.")
		return nil
	}
	endpoint, ok := event.Extra["endpoint_id"]
	if !ok {
		logger.Errorf("event does not have endpoint_id set.")
		return nil
	}
	monitor, ok := event.Extra["monitor_id"]
	if !ok {
		logger.Errorf("event does not have monitor_id set.")
		return nil
	}
	logger.Debugf("updating state of monitor: %v from %v", monitor, event.Extra["collector"])
	payload := &StateChange{
		OrgId: event.OrgId,
		EndpointId: endpoint.(float64),
		MonitorId: monitor.(float64),
		CollectorId: collector.(float64),
		Updated: time.Unix(0, event.Timestamp*int64(time.Millisecond)),
	}
	// update the check state
	switch event.Severity {
	case "OK":
		payload.State = 0
	case "WARN":
		payload.State = 1
	case "ERROR":
		payload.State = 2
	default:
		payload.State = -1
	}
	
	payloadBuf, err := json.Marshal(payload)
	if err != nil {
		logger.Errorf("Could not marshal state update payload.", err)
		return nil
	}
	client := &http.Client{}
	req, err := http.NewRequest("POST", config.ApiUrl + "/api/monitors/state", bytes.NewBuffer(payloadBuf))
	req.Header.Add("Authorization", "Bearer "+config.ApiKey)
	req.Header.Add("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		logger.Errorf("state update request failed.", err)
		return nil
	}
	if resp.StatusCode != 200 {
		logger.Debugf("state update failed.")
		body, err := ioutil.ReadAll(resp.Body)
		if err != nil {
			logger.Errorf("Failed to read body of response.", err)
		}
		resp.Body.Close()
		logger.Debugf("%s", body)
	}

	return nil
}