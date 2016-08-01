package mdata

import (
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/raintank/met"
	"github.com/raintank/worldping-api/pkg/log"
)

var (
	CluStatus           *ClusterStatus
	clusterHandlers     []ClusterHandler
	persistMessageBatch *PersistMessageBatch

	messagesPublished met.Count
	messagesSize      met.Meter
)

type ClusterHandler interface {
	Send(SavedChunk)
}

//PersistMessage format version
const PersistMessageBatchV1 = 1

// ClusterStatus has Exported fields but don't touch them directly
// it's only for json marshaling. use the accessor methods.
type ClusterStatus struct {
	sync.Mutex
	Instance   string    `json:"instance"`
	Primary    bool      `json:"primary"`
	LastChange time.Time `json:"lastChange"`
}

func NewClusterStatus(instance string, initialState bool) *ClusterStatus {
	return &ClusterStatus{
		Instance:   instance,
		Primary:    initialState,
		LastChange: time.Now(),
	}
}

func (c *ClusterStatus) Marshal() ([]byte, error) {
	c.Lock()
	defer c.Unlock()
	return json.Marshal(c)
}

func (c *ClusterStatus) Set(newState bool) {
	c.Lock()
	c.Primary = newState
	c.LastChange = time.Now()
	c.Unlock()
}

func (c *ClusterStatus) IsPrimary() bool {
	c.Lock()
	defer c.Unlock()
	return c.Primary
}

// Handle requests for /cluster. POST to set primary flag, GET to get current state.
func (c *ClusterStatus) HttpHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method == "GET" {
		c.getClusterStatus(w, req)
		return
	}
	if req.Method == "POST" {
		c.setClusterStatus(w, req)
		return
	}
	http.Error(w, "not found.", http.StatusNotFound)
}

func (c *ClusterStatus) setClusterStatus(w http.ResponseWriter, req *http.Request) {
	req.ParseForm()
	newState := req.Form.Get("primary")
	if newState == "" {
		http.Error(w, "primary field missing from payload.", http.StatusBadRequest)
		return
	}

	primary, err := strconv.ParseBool(newState)
	if err != nil {
		http.Error(w, "primary field could not be parsed to boolean value.", http.StatusBadRequest)
		return
	}
	c.Set(primary)
	log.Info("primary status is now %t", primary)
	w.Write([]byte("OK"))
}

func (c *ClusterStatus) getClusterStatus(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	resp, err := c.Marshal()
	if err != nil {
		http.Error(w, "could not marshal status to json", http.StatusInternalServerError)
		return
	}
	w.Write(resp)
}

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
	PartKey []byte `json:"-"`
	Key     string `json:"key"`
	T0      uint32 `json:"t0"`
}

func SendPersistMessage(key string, t0 uint32, partKey []byte) {
	sc := SavedChunk{Key: key, T0: t0, PartKey: partKey}
	for _, h := range clusterHandlers {
		h.Send(sc)
	}
}

func InitCluster(stats met.Backend, handlers ...ClusterHandler) {
	messagesPublished = stats.NewCount("cluster.messages-published")
	messagesSize = stats.NewMeter("cluster.message_size", 0)
	clusterHandlers = handlers
}

// Cl is a reusable component that will handle metricpersist messages
type Cl struct {
	instance string
	metrics  Metrics
}

func NewCl(instance string, metrics Metrics) Cl {
	return Cl{
		instance: instance,
		metrics:  metrics,
	}
}

func (cl Cl) Handle(data []byte) {
	version := uint8(data[0])
	if version == uint8(PersistMessageBatchV1) {
		// new batch format.
		batch := PersistMessageBatch{}
		err := json.Unmarshal(data[1:], &batch)
		if err != nil {
			log.Error(3, "failed to unmarsh batch message. skipping.", err)
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
			log.Error(3, "skipping message. %s", err)
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
