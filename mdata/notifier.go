package mdata

// this file is for "clustering" in the sense of keeping other instances who host the same data, aware of what data has been persisted and which has not

import (
	"encoding/json"

	"github.com/raintank/met"
	"github.com/raintank/worldping-api/pkg/log"
)

var (
	notifierHandlers    []NotifierHandler
	persistMessageBatch *PersistMessageBatch

	messagesPublished met.Count
	messagesSize      met.Meter
)

type NotifierHandler interface {
	Send(SavedChunk)
}

//PersistMessage format version
const PersistMessageBatchV1 = 1

type PersistMessage struct {
	Instance string `json:"instance"`
	Key      string `json:"key"`
	T0       uint32 `json:"t0"`
}

type PersistMessageBatch struct {
	Instance    string       `json:"instance"`
	SavedChunks []SavedChunk `json:"saved_chunks"`
}

type SavedChunk struct {
	Key string `json:"key"`
	T0  uint32 `json:"t0"`
}

func SendPersistMessage(key string, t0 uint32) {
	sc := SavedChunk{Key: key, T0: t0}
	for _, h := range notifierHandlers {
		h.Send(sc)
	}
}

func InitPersistNotifier(stats met.Backend, handlers ...NotifierHandler) {
	messagesPublished = stats.NewCount("cluster.messages-published")
	messagesSize = stats.NewMeter("cluster.message_size", 0)
	notifierHandlers = handlers
}

type Notifier struct {
	instance string
	metrics  Metrics
}

func (cl Notifier) Handle(data []byte) {
	version := uint8(data[0])
	if version == uint8(PersistMessageBatchV1) {
		// new batch format.
		batch := PersistMessageBatch{}
		err := json.Unmarshal(data[1:], &batch)
		if err != nil {
			log.Error(3, "CLU failed to unmarsh batch message. skipping.", err)
			return
		}
		if batch.Instance == cl.instance {
			log.Debug("CLU skipping batch message we generated.")
			return
		}
		for _, c := range batch.SavedChunks {
			if agg, ok := cl.metrics.Get(c.Key); ok {
				agg.(*AggMetric).SyncChunkSaveState(c.T0)
			}
		}
	} else {
		// assume the old format.
		ms := PersistMessage{}
		err := json.Unmarshal(data, &ms)
		if err != nil {
			log.Error(3, "CLU skipping message. %s", err)
			return
		}
		if ms.Instance == cl.instance {
			log.Debug("CLU skipping message we generated. %s - %s:%d", ms.Instance, ms.Key, ms.T0)
			return
		}

		// get metric
		if agg, ok := cl.metrics.Get(ms.Key); ok {
			agg.(*AggMetric).SyncChunkSaveState(ms.T0)
		}
	}
	return
}
