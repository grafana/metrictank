package mdata

import (
	"encoding/json"

	"github.com/raintank/metrictank/stats"
	"github.com/raintank/worldping-api/pkg/log"
)

var (
	notifierHandlers    []NotifierHandler
	persistMessageBatch *PersistMessageBatch

	// metric cluster.notifier.all.messages-received is a counter of messages received from cluster notifiers
	messagesReceived = stats.NewCounter32("cluster.notifier.all.messages-received")
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

func InitPersistNotifier(handlers ...NotifierHandler) {
	notifierHandlers = handlers
}

type Notifier struct {
	Instance             string
	Metrics              Metrics
	CreateMissingMetrics bool
}

func (cl Notifier) Handle(data []byte) {
	version := uint8(data[0])
	if version == uint8(PersistMessageBatchV1) {
		// new batch format.
		batch := PersistMessageBatch{}
		err := json.Unmarshal(data[1:], &batch)
		if err != nil {
			log.Error(3, "failed to unmarsh batch message. skipping.", err)
			return
		}
		messagesReceived.Add(len(batch.SavedChunks))
		for _, c := range batch.SavedChunks {
			if cl.CreateMissingMetrics {
				agg := cl.Metrics.GetOrCreate(c.Key)
				agg.(*AggMetric).SyncChunkSaveState(c.T0)
			} else if agg, ok := cl.Metrics.Get(c.Key); ok {
				agg.(*AggMetric).SyncChunkSaveState(c.T0)
			}
		}
	} else {
		// assume the old format.
		ms := PersistMessage{}
		err := json.Unmarshal(data, &ms)
		if err != nil {
			log.Error(3, "skipping message. %s", err)
			return
		}
		messagesReceived.Add(1)
		// get metric
		if cl.CreateMissingMetrics {
			agg := cl.Metrics.GetOrCreate(ms.Key)
			agg.(*AggMetric).SyncChunkSaveState(ms.T0)
		} else if agg, ok := cl.Metrics.Get(ms.Key); ok {
			agg.(*AggMetric).SyncChunkSaveState(ms.T0)
		}
	}
	return
}
