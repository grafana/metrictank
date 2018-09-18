package mdata

import (
	"encoding/json"

	"github.com/raintank/schema"

	"github.com/grafana/metrictank/consolidation"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/stats"
	log "github.com/sirupsen/logrus"
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
			log.WithFields(log.Fields{
				"error": err.Error(),
			}).Error("notifier: failed to unmarshal batch message, skipping")
			return
		}
		messagesReceived.Add(len(batch.SavedChunks))
		for _, c := range batch.SavedChunks {
			amkey, err := schema.AMKeyFromString(c.Key)
			if err != nil {
				log.WithFields(log.Fields{
					"key":   c.Key,
					"error": err.Error(),
				}).Error("notifier: failed to convert key to AMKey, skipping")
				continue
			}
			// we only need to handle saves for series that we know about.
			// if the series is not in the index, then we dont need to worry about it.
			def, ok := idx.Get(amkey.MKey)
			if !ok {
				log.WithFields(log.Fields{
					"mkey": amkey.MKey,
				}).Debug("notifier: skipping metric for mkey as it is not in the index")
				continue
			}
			agg := metrics.GetOrCreate(amkey.MKey, def.SchemaId, def.AggId)
			if amkey.Archive != 0 {
				consolidator := consolidation.FromArchive(amkey.Archive.Method())
				aggSpan := amkey.Archive.Span()
				agg.(*AggMetric).SyncAggregatedChunkSaveState(c.T0, consolidator, aggSpan)
			} else {
				agg.(*AggMetric).SyncChunkSaveState(c.T0)
			}
		}
	} else {
		log.WithFields(log.Fields{
			"version": version,
		}).Error("notifier: unknown version")
	}
	return
}
