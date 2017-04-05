package mdata

import (
	"encoding/json"
	"strconv"
	"strings"

	"github.com/raintank/metrictank/consolidation"
	"github.com/raintank/metrictank/idx"
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

type PersistMessageBatch struct {
	Instance    string       `json:"instance"`
	SavedChunks []SavedChunk `json:"saved_chunks"`
}

type SavedChunk struct {
	Key  string `json:"key"`
	T0   uint32 `json:"t0"`
	Name string `json:"name"`
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

func Handle(metrics Metrics, data []byte, idx idx.MetricIndex, createMissing bool) {
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
			if createMissing {
				// support pre-8f800f344f7c9906f32a819134224992931e3a5d producers that don't set the Name property
				if c.Name == "" {
					def, ok := idx.Get(key[0])
					if !ok {
						log.Error(3, "cluster handler: failed to lookup metricDef with id %s", c.Key)
						return
					}
					c.Name = def.Name
				}

				schemaId, _ := MatchSchema(c.Name)
				aggId, _ := MatchAgg(c.Name)
				agg := metrics.GetOrCreate(key[0], c.Name, schemaId, aggId)
				if len(key) == 3 {
					agg.(*AggMetric).SyncAggregatedChunkSaveState(c.T0, consolidator, uint32(aggSpan))
				} else {
					agg.(*AggMetric).SyncChunkSaveState(c.T0)
				}
			} else if agg, ok := metrics.Get(key[0]); ok {
				if len(key) > 1 {
					agg.(*AggMetric).SyncAggregatedChunkSaveState(c.T0, consolidator, uint32(aggSpan))
				} else {
					agg.(*AggMetric).SyncChunkSaveState(c.T0)
				}
			}
		}
	} else {
		log.Error(3, "notifier: unknown version %d", version)
	}
	return
}
