package mdata

import (
	"encoding/json"

	"github.com/grafana/metrictank/pkg/schema"

	"github.com/grafana/metrictank/pkg/consolidation"
	"github.com/grafana/metrictank/pkg/idx"
	"github.com/grafana/metrictank/pkg/stats"
	log "github.com/sirupsen/logrus"
)

var (
	notifiers []Notifier

	// metric cluster.notifier.all.messages-received is a counter of messages received from cluster notifiers
	messagesReceived = stats.NewCounter32("cluster.notifier.all.messages-received")
)

type Notifier interface {
	Send(SavedChunk)
}

// PersistMessage format version
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
	for _, h := range notifiers {
		h.Send(sc)
	}
}

func InitPersistNotifier(not ...Notifier) {
	notifiers = not
}

type NotifierHandler interface {
	// Handle handles an incoming message
	Handle([]byte)
	// PartitionOf is used for notifiers that want to flush and need partition information for metrics
	PartitionOf(key schema.MKey) (int32, bool)
}

type DefaultNotifierHandler struct {
	idx     idx.MetricIndex
	metrics Metrics
}

func NewDefaultNotifierHandler(metrics Metrics, idx idx.MetricIndex) DefaultNotifierHandler {
	return DefaultNotifierHandler{
		idx:     idx,
		metrics: metrics,
	}
}

func (dn DefaultNotifierHandler) PartitionOf(key schema.MKey) (int32, bool) {
	def, ok := dn.idx.Get(key)
	return def.Partition, ok
}

func (dn DefaultNotifierHandler) Handle(data []byte) {
	version := uint8(data[0])
	if version == uint8(PersistMessageBatchV1) {
		batch := PersistMessageBatch{}
		err := json.Unmarshal(data[1:], &batch)
		if err != nil {
			log.Errorf("failed to unmarsh batch message: %s -- skipping", err)
			return
		}
		messagesReceived.Add(len(batch.SavedChunks))
		for _, c := range batch.SavedChunks {
			amkey, err := schema.AMKeyFromString(c.Key)
			if err != nil {
				log.Errorf("notifier: failed to convert %q to AMKey: %s -- skipping", c.Key, err)
				continue
			}
			// we only need to handle saves for series that we know about.
			// if the series is not in the index, then we dont need to worry about it.
			def, ok := dn.idx.Get(amkey.MKey)
			if !ok {
				log.Debugf("notifier: skipping metric with MKey %s as it is not in the index", amkey.MKey)
				continue
			}
			agg := dn.metrics.GetOrCreate(amkey.MKey, def.SchemaId, def.AggId, uint32(def.Interval))
			if amkey.Archive != 0 {
				consolidator := consolidation.FromArchive(amkey.Archive.Method())
				aggSpan := amkey.Archive.Span()
				agg.(*AggMetric).SyncAggregatedChunkSaveState(c.T0, consolidator, aggSpan)
			} else {
				agg.(*AggMetric).SyncChunkSaveState(c.T0, false)()
			}
		}
	} else {
		log.Errorf("notifier: unknown version %d", version)
	}
	return
}
