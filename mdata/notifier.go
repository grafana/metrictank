package mdata

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/raintank/metrictank/consolidation"
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
			key := strings.Split(c.Key, "_")
			var consolidator consolidation.Consolidator
			var aggSpan int
			if len(key) == 3 {
				consolidator = consolidation.FromArchive(key[1])
				aggSpan, err = strconv.Atoi(key[2])
				if err != nil {
					log.Error(3, "notifier: skipping message due to parsing failure. %s", err)
					continue
				}
			}
			if cl.CreateMissingMetrics {
				agg := cl.Metrics.GetOrCreate(key[0])
				if len(key) == 3 {
					agg.(*AggMetric).SyncAggregatedChunkSaveState(c.T0, consolidator, uint32(aggSpan))
				} else {
					agg.(*AggMetric).SyncChunkSaveState(c.T0)
				}
			} else if agg, ok := cl.Metrics.Get(key[0]); ok {
				if len(key) > 1 {
					agg.(*AggMetric).SyncAggregatedChunkSaveState(c.T0, consolidator, uint32(aggSpan))
				} else {
					agg.(*AggMetric).SyncChunkSaveState(c.T0)
				}
			}
		}
	} else {
		// assume the old format.
		ms := PersistMessage{}
		err := json.Unmarshal(data, &ms)
		if err != nil {
			log.Error(3, "notifier: skipping message due to parsing failure. %s", err)
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
