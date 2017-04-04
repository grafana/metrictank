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

type PersistMessage struct {
	Instance string `json:"instance"`
	Key      string `json:"key"`
	Name     string `json:"name"`
	T0       uint32 `json:"t0"`
	Interval int    `json:"interval"`
}

type PersistMessageBatch struct {
	Instance    string       `json:"instance"`
	SavedChunks []SavedChunk `json:"saved_chunks"`
}

type SavedChunk struct {
	Key string `json:"key"`
	T0  uint32 `json:"t0"`

	Name     string `json:"name"`     // filled in when handler does index lookup
	Interval int    `json:"interval"` // filled in when handler does index lookup
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
	Idx                  idx.MetricIndex
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
			if c.Name == "" || c.Interval == 0 {
				def, ok := cl.Idx.Get(key[0])
				if !ok {
					log.Error(3, "notifier: failed to lookup metricDef with id %s", key[0])
					continue
				}
				c.Name = def.Name
				c.Interval = def.Interval
			}
			if cl.CreateMissingMetrics {
				schemaId, _ := MatchSchema(c.Name, c.Interval)
				aggId, _ := MatchAgg(c.Name)
				agg := cl.Metrics.GetOrCreate(key[0], c.Name, schemaId, aggId)
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
		if ms.Name == "" || ms.Interval == 0 {
			key := strings.SplitN(ms.Key, "_", 2)[0]
			def, ok := cl.Idx.Get(key)
			if !ok {
				log.Error(3, "notifier: failed to lookup metricDef with id %s", key)
				return
			}
			ms.Name = def.Name
			ms.Interval = def.Interval
		}
		messagesReceived.Add(1)
		// get metric
		if cl.CreateMissingMetrics {
			schemaId, _ := MatchSchema(ms.Name, ms.Interval)
			aggId, _ := MatchAgg(ms.Name)
			agg := cl.Metrics.GetOrCreate(ms.Key, ms.Name, schemaId, aggId)
			agg.(*AggMetric).SyncChunkSaveState(ms.T0)
		} else if agg, ok := cl.Metrics.Get(ms.Key); ok {
			agg.(*AggMetric).SyncChunkSaveState(ms.T0)
		}
	}
	return
}
