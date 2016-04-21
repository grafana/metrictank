package mdata

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/bitly/go-hostpool"
	"github.com/grafana/grafana/pkg/log"
	"github.com/nsqio/go-nsq"
	"github.com/raintank/met"
	"github.com/raintank/raintank-metric/app"
	"github.com/raintank/raintank-metric/instrumented_nsq"
)

var (
	hostPool            hostpool.HostPool
	producers           map[string]*nsq.Producer
	CluStatus           *ClusterStatus
	persistMessageBatch *PersistMessageBatch
)

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

type PersistMessage struct {
	Instance string `json:"instance"`
	Key      string `json:"key"`
	T0       uint32 `json:"t0"`
}

type PersistMessageBatch struct {
	sync.Mutex  `json:"-"`
	Topic       string       `json:"-"`
	Instance    string       `json:"instance"`
	SavedChunks []savedChunk `json:"saved_chunks"`
}

type savedChunk struct {
	Key string `json:"key"`
	T0  uint32 `json:"t0"`
}

func (p *PersistMessageBatch) AddChunk(key string, t0 uint32) {
	p.Lock()
	defer p.Unlock()
	p.SavedChunks = append(p.SavedChunks, savedChunk{Key: key, T0: t0})
}

func SendPersistMessage(key string, t0 uint32) {
	persistMessageBatch.AddChunk(key, t0)
}

func (p *PersistMessageBatch) flush() {
	ticker := time.NewTicker(time.Second)
	for range ticker.C {
		p.Lock()
		msg := PersistMessageBatch{Instance: p.Instance, SavedChunks: p.SavedChunks}
		p.SavedChunks = nil
		p.Unlock()

		if len(msg.SavedChunks) == 0 {
			continue
		}

		if p.Topic == "" {
			continue
		}

		log.Debug("CLU sending %d batch metricPersist messages", len(msg.SavedChunks))

		data, err := json.Marshal(&msg)
		if err != nil {
			log.Fatal(4, "failed to marshal persistMessage to json.")
		}
		buf := new(bytes.Buffer)
		binary.Write(buf, binary.LittleEndian, uint8(PersistMessageBatchV1))
		buf.Write(data)

		sent := false
		for !sent {
			// This will always return a host. If all hosts are currently marked as dead,
			// then all hosts will be reset to alive and we will try them all again. This
			// will result in this loop repeating forever until we successfully publish our msg.
			hostPoolResponse := hostPool.Get()
			prod := producers[hostPoolResponse.Host()]
			err = prod.Publish(p.Topic, buf.Bytes())
			// Hosts that are marked as dead will be retried after 30seconds.  If we published
			// successfully, then sending a nil error will mark the host as alive again.
			hostPoolResponse.Mark(err)
			if err != nil {
				log.Warn("publisher marking host %s as faulty due to %s", hostPoolResponse.Host(), err)
			} else {
				sent = true
			}
		}
	}
}

type MetricPersistHandler struct {
	instance string
	metrics  Metrics
}

func NewMetricPersistHandler(instance string, metrics Metrics) *MetricPersistHandler {
	return &MetricPersistHandler{
		instance: instance,
		metrics:  metrics,
	}
}

func (h *MetricPersistHandler) HandleMessage(m *nsq.Message) error {
	version := uint8(m.Body[0])
	if version == uint8(PersistMessageBatchV1) {
		// new batch format.
		batch := PersistMessageBatch{}
		err := json.Unmarshal(m.Body[1:], &batch)
		if err != nil {
			log.Error(3, "failed to unmarsh batch message. skipping.", err)
			return nil
		}
		if batch.Instance == h.instance {
			log.Debug("CLU skipping batch message we generated.")
			return nil
		}
		for _, c := range batch.SavedChunks {
			if agg, ok := h.metrics.Get(c.Key); ok {
				agg.(*AggMetric).SyncChunkSaveState(c.T0)
			}
		}
	} else {
		// assume the old format.
		ms := PersistMessage{}
		err := json.Unmarshal(m.Body, &ms)
		if err != nil {
			log.Error(3, "skipping message. %s", err)
			return nil
		}
		if ms.Instance == h.instance {
			log.Debug("CLU skipping message we generated. %s - %s:%d", ms.Instance, ms.Key, ms.T0)
			return nil
		}

		// get metric
		if agg, ok := h.metrics.Get(ms.Key); ok {
			agg.(*AggMetric).SyncChunkSaveState(ms.T0)
		}
	}
	return nil
}

func InitCluster(metrics Metrics, stats met.Backend, instance, topic, channel, producerOpts, consumerOpts string, nsqdAdds, lookupdAdds []string, maxInFlight int) {
	persistMessageBatch = &PersistMessageBatch{Instance: instance, SavedChunks: make([]savedChunk, 0), Topic: topic}
	// init producers
	pCfg := nsq.NewConfig()
	pCfg.UserAgent = "metrics_tank"
	err := app.ParseOpts(pCfg, producerOpts)
	if err != nil {
		log.Fatal(4, "failed to parse nsq producer options. %s", err)
	}
	hostPool = hostpool.NewEpsilonGreedy(nsqdAdds, 0, &hostpool.LinearEpsilonValueCalculator{})
	producers = make(map[string]*nsq.Producer)

	for _, addr := range nsqdAdds {
		producer, err := nsq.NewProducer(addr, pCfg)
		if err != nil {
			log.Fatal(4, "failed creating producer %s", err.Error())
		}
		producers[addr] = producer
	}

	// init consumers
	cfg := nsq.NewConfig()
	cfg.UserAgent = "metrics_tank_cluster"
	err = app.ParseOpts(cfg, consumerOpts)
	if err != nil {
		log.Fatal(4, "failed to parse nsq consumer options. %s", err)
	}
	cfg.MaxInFlight = maxInFlight

	consumer, err := insq.NewConsumer(topic, channel, cfg, "metric_persist.%s", stats)
	if err != nil {
		log.Fatal(4, "Failed to create NSQ consumer. %s", err)
	}
	handler := NewMetricPersistHandler(instance, metrics)
	consumer.AddConcurrentHandlers(handler, 2)

	err = consumer.ConnectToNSQDs(nsqdAdds)
	if err != nil {
		log.Fatal(4, "failed to connect to NSQDs. %s", err)
	}
	log.Info("persist consumer connected to nsqd")

	err = consumer.ConnectToNSQLookupds(lookupdAdds)
	if err != nil {
		log.Fatal(4, "failed to connect to NSQLookupds. %s", err)
	}
	go persistMessageBatch.flush()
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
