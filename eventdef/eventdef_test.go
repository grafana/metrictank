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

package eventdef

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"testing"
	"time"

	"github.com/bmizerany/assert"
	"github.com/codeskyblue/go-uuid"
	"github.com/raintank/raintank-metric/schema"
)

func makeEvent(timestamp time.Time) *schema.ProbeEvent {
	e := new(schema.ProbeEvent)
	e.Id = uuid.NewUUID().String()
	e.EventType = "monitor_state"
	e.OrgId = 1
	e.Severity = "ERROR"
	e.Source = "network_collector"
	e.Message = "100% packet loss"
	e.Timestamp = timestamp.UnixNano() / int64(time.Millisecond)

	return e
}

func parseRequestPayload(buf *bytes.Buffer) map[string][]*schema.ProbeEvent {
	/*
		{"index":{"_index":"events-2016-01-12","_type":"monitor_state","_id":"f6698a1d-b8ef-11e5-b991-74d4353d029e","_timestamp":"1452577647000"}}
		{"id":"f6698a1d-b8ef-11e5-b991-74d4353d029e","event_type":"monitor_state","org_id":1,"severity":"ERROR","source":"network_collector","timestamp":1452577647547,"message":"100% packet loss","tags":null}
	*/
	resp := make(map[string][]*schema.ProbeEvent)
	lines := strings.Split(buf.String(), "\n")
	for i := 0; i < len(lines); i += 2 {
		if lines[i] == "" {
			continue
		}
		idx := make(map[string]map[string]string)
		err := json.Unmarshal([]byte(lines[i]), &idx)
		if err != nil {
			log.Fatalf("failed to marshal payload line. %s", err)
		}
		index := idx["index"]["_index"]
		event := new(schema.ProbeEvent)
		err = json.Unmarshal([]byte(lines[i+1]), event)
		if err != nil {
			log.Fatalf("failed to marshal payload line.%s", err)
		}
		if _, ok := resp[index]; !ok {
			resp[index] = make([]*schema.ProbeEvent, 0)
		}
		resp[index] = append(resp[index], event)

	}
	return resp
}

func TestPayloadSentOneEvent(t *testing.T) {
	eTime := time.Now()
	e := makeEvent(eTime)
	sender := func(buf *bytes.Buffer) error {
		//log.Printf("sending bytes. %s", buf)
		data := parseRequestPayload(buf)
		assert.T(t, len(data) == 1, fmt.Sprintf("Should have been only one indexName, found  %d", len(data)))
		y, m, d := eTime.Date()
		idxName := fmt.Sprintf("events-%d-%02d-%02d", y, m, d)
		events, indexFound := data[idxName]
		assert.T(t, indexFound, fmt.Sprintf("index %s not found in request payload.", idxName))
		assert.T(t, len(events) == 1, fmt.Sprintf("payload should have 1 event. %d found", len(events)))
		assert.T(t, events[0].Id == e.Id, fmt.Sprintf("expected event id %s, got %s instead", e.Id, events[0].Id))
		return nil
	}
	initBulkIndexer(sender, 10)
	err := Save(e)
	if err != nil {
		t.Errorf("error right off the bat with saving: %s", err.Error())
	}

	bulk.Stop()
}

func TestPayloadSentMultipleEvents(t *testing.T) {
	eTime := time.Now()
	events := make([]*schema.ProbeEvent, 5)
	for i := 0; i < 5; i++ {
		events[i] = makeEvent(eTime)
	}

	sender := func(buf *bytes.Buffer) error {
		//log.Printf("sending bytes. %s", buf)
		data := parseRequestPayload(buf)
		assert.T(t, len(data) == 1, fmt.Sprintf("Should have been only one indexName, found  %d", len(data)))
		y, m, d := eTime.Date()
		idxName := fmt.Sprintf("events-%d-%02d-%02d", y, m, d)
		sentEvents, indexFound := data[idxName]
		assert.T(t, indexFound, fmt.Sprintf("index %s not found in request payload.", idxName))

		assert.T(t, len(sentEvents) == 5, fmt.Sprintf("payload should have 1 event. %d found", len(sentEvents)))
		for i, e := range events {
			assert.T(t, sentEvents[i].Id == e.Id, fmt.Sprintf("expected event id %s, got %s instead", e.Id, sentEvents[i].Id))
		}

		return nil
	}

	initBulkIndexer(sender, 10)
	for _, e := range events {
		err := Save(e)
		if err != nil {
			t.Errorf("error right off the bat with saving: %s", err.Error())
		}
	}

	bulk.Stop()
}

func TestResponseChannelOk(t *testing.T) {
	writeStatus = make(chan *BulkSaveStatus, 1)

	esResponse := []byte(`{
    "took": 1,
    "errors": false,
    "items": [
        {
            "index": {
                "_index": "events-2016-01-12",
                "_type": "monitor_state",
                "_id": "1",
                "_version": 2,
                "status": 200
            }
        }
    ]
}`)
	processEsResponse(esResponse)

	status := <-writeStatus
	assert.T(t, status.Id == "1", fmt.Sprintf("expected status for event %s, got %s.", "1", status.Id))
	assert.T(t, status.Ok, fmt.Sprintf("expected status.Ok to be true."))
}

func TestResponseChannelFailed(t *testing.T) {
	writeStatus = make(chan *BulkSaveStatus, 1)

	esResponse := []byte(`{
    "took": 1,
    "errors": true,
    "items": [
        {
            "index": {
                "_index": "events-2016-01-12",
                "_type": "monitor_state",
                "_id": "1",
                "status": 409,
                "error": "DocumentAlreadyExistsException[[events-2016-01-12][1] [monitor_state][f6698a1d-b8ef-11e5-b991-74d4353d029e]: document already exists]"
            }
        }
    ]
}`)
	processEsResponse(esResponse)

	status := <-writeStatus
	assert.T(t, status.Id == "1", fmt.Sprintf("expected status for event %s, got %s.", "1", status.Id))
	assert.T(t, !status.Ok, fmt.Sprintf("expected status.Ok to be false."))
}

func TestResponseChannelOkAndFailed(t *testing.T) {
	writeStatus = make(chan *BulkSaveStatus, 1)

	esResponse := []byte(`{
    "took": 1,
    "errors": true,
    "items": [
        {
            "index": {
                "_index": "events-2016-01-12",
                "_type": "monitor_state",
                "_id": "1",
                "status": 409,
                "error": "DocumentAlreadyExistsException[[events-2016-01-12][1] [monitor_state][f6698a1d-b8ef-11e5-b991-74d4353d029e]: document already exists]"
            }
        },
        {
            "index": {
                "_index": "events-2016-01-12",
                "_type": "monitor_state",
                "_id": "2",
                "_version": 2,
                "status": 200
            }
        }
    ]
}`)
	go processEsResponse(esResponse)

	status := <-writeStatus
	assert.T(t, status.Id == "1", fmt.Sprintf("expected status for event %s, got %s.", "1", status.Id))
	assert.T(t, !status.Ok, fmt.Sprintf("expected status.Ok to be false."))

	status = <-writeStatus
	assert.T(t, status.Id == "2", fmt.Sprintf("expected status for event %s, got %s.", "2", status.Id))
	assert.T(t, status.Ok, fmt.Sprintf("expected status.Ok to be true."))
}
