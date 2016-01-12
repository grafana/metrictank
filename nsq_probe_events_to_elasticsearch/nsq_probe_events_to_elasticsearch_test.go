package main

import (
	"fmt"
	"testing"
	"time"

	"github.com/bmizerany/assert"
	"github.com/codeskyblue/go-uuid"
	"github.com/grafana/grafana/pkg/metric/helper"
	"github.com/raintank/raintank-metric/eventdef"
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

func TestAddInProgressMessage(t *testing.T) {
	e := makeEvent(time.Now())
	metrics, _ := helper.New(false, "", "standard", "nsq_prove_events_to_elasticsearch", "")
	initMetrics(metrics)

	writeQueue = NewInProgressMessageQueue()

	writeQueue.EnQueue(e.Id, nil)

	assert.T(t, len(writeQueue.inProgress) == 1, fmt.Sprintf("expected 1 item in inProgress queue. found %d", len(writeQueue.inProgress)))
}

func TestAddInProgressMessageThenProcess(t *testing.T) {
	e := makeEvent(time.Now())
	metrics, _ := helper.New(false, "", "standard", "nsq_prove_events_to_elasticsearch", "")
	initMetrics(metrics)

	writeQueue = NewInProgressMessageQueue()

	writeQueue.EnQueue(e.Id, nil)

	writeQueue.Lock()
	assert.T(t, len(writeQueue.inProgress) == 1, fmt.Sprintf("expected 1 item in inProgress queue. found %d", len(writeQueue.inProgress)))
	writeQueue.Unlock()

	//push a saveStatus to the status Channel
	writeQueue.status <- &eventdef.BulkSaveStatus{
		Id: e.Id,
		Ok: true,
	}

	time.Sleep(time.Millisecond)
	writeQueue.Lock()
	assert.T(t, len(writeQueue.inProgress) == 0, fmt.Sprintf("expected 0 items in inProgress queue. found %d", len(writeQueue.inProgress)))
	writeQueue.Unlock()
}

func TestAddInProgressMessageThenProcessFailed(t *testing.T) {
	e := makeEvent(time.Now())
	metrics, _ := helper.New(false, "", "standard", "nsq_prove_events_to_elasticsearch", "")
	initMetrics(metrics)

	writeQueue = NewInProgressMessageQueue()

	writeQueue.EnQueue(e.Id, nil)

	writeQueue.Lock()
	assert.T(t, len(writeQueue.inProgress) == 1, fmt.Sprintf("expected 1 item in inProgress queue. found %d", len(writeQueue.inProgress)))
	writeQueue.Unlock()

	//push a saveStatus to the status Channel
	writeQueue.status <- &eventdef.BulkSaveStatus{
		Id: e.Id,
		Ok: false,
	}

	time.Sleep(time.Millisecond)
	writeQueue.Lock()
	assert.T(t, len(writeQueue.inProgress) == 0, fmt.Sprintf("expected 0 items in inProgress queue. found %d", len(writeQueue.inProgress)))
	writeQueue.Unlock()
}

func TestAddMultipleInProgressMessageThenProcess(t *testing.T) {

	metrics, _ := helper.New(false, "", "standard", "nsq_prove_events_to_elasticsearch", "")
	initMetrics(metrics)

	writeQueue = NewInProgressMessageQueue()
	events := make([]*schema.ProbeEvent, 100)
	for i := 0; i < 100; i++ {
		e := makeEvent(time.Now())
		events[i] = e
		go writeQueue.EnQueue(e.Id, nil)
	}
	time.Sleep(time.Millisecond)

	writeQueue.Lock()
	assert.T(t, len(writeQueue.inProgress) == 100, fmt.Sprintf("expected 100 item in inProgress queue. found %d", len(writeQueue.inProgress)))
	writeQueue.Unlock()

	for _, e := range events {
		//push a saveStatus to the status Channel
		writeQueue.status <- &eventdef.BulkSaveStatus{
			Id: e.Id,
			Ok: true,
		}
	}

	time.Sleep(time.Second)
	writeQueue.Lock()
	assert.T(t, len(writeQueue.inProgress) == 0, fmt.Sprintf("expected 0 items in inProgress queue. found %d", len(writeQueue.inProgress)))
	writeQueue.Unlock()
}
