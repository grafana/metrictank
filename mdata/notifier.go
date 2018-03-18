package mdata

import (
	"encoding/json"
	"strconv"
	"strings"

	schema "gopkg.in/raintank/schema.v1"

	"github.com/grafana/metrictank/consolidation"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/stats"
	"github.com/raintank/worldping-api/pkg/log"
)

var (
	notifierHandlers []NotifierHandler

	// metric cluster.notifier.all.messages-received is a counter of messages received from cluster notifiers
	messagesReceived = stats.NewCounter32("cluster.notifier.all.messages-received")
)

type NotifierHandler interface {
	Send(SavedChunk)
}

//PersistMessage format version
const PersistMessageBatchV1 = 1

type PersistMessageBatch struct {
	Instance    string       `json:"instance"`
	SavedChunks []SavedChunk `json:"saved_chunks"`
}

// SavedChunk represents a chunk persisted to the store
// Key is a stringified schema.AMKey
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

func Handle(metrics Metrics, data []byte, idx idx.MetricIndex) {
	version := uint8(data[0])
	if version == uint8(PersistMessageBatchV1) {
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
			mkey, err := schema.MKeyFromString(key[0])
			if err != nil {
				log.Error(3, "notifier: failed to convert %q to MKey: %s", c.Key, err)
				continue
			}
			// we only need to handle saves for series that we know about.
			// if the series is not in the index, then we dont need to worry about it.
			def, ok := idx.Get(mkey)
			if !ok {
				log.Debug("notifier: skipping metric with id %s as it is not in the index", key[0])
				continue
			}
			agg := metrics.GetOrCreate(mkey, def.SchemaId, def.AggId)
			if len(key) == 3 {
				agg.(*AggMetric).SyncAggregatedChunkSaveState(c.T0, consolidator, uint32(aggSpan))
			} else {
				agg.(*AggMetric).SyncChunkSaveState(c.T0)
			}
		}
	} else {
		log.Error(3, "notifier: unknown version %d", version)
	}
	return
}
