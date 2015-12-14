package main

import (
	"encoding/json"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/bitly/go-hostpool"
	"github.com/grafana/grafana/pkg/log"
	met "github.com/grafana/grafana/pkg/metric"
	"github.com/nsqio/go-nsq"
	"github.com/raintank/raintank-metric/app"
	"github.com/raintank/raintank-metric/instrumented_nsq"
)

var (
	hostPool      hostpool.HostPool
	producers     map[string]*nsq.Producer
	clusterStatus *ClusterStatus
)

type ClusterStatus struct {
	sync.Mutex
	Instance   string
	Primary    bool
	LastChange time.Time
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

func (msg *PersistMessage) Send() {
	if *topicNotifyPersist == "" {
		return
	}
	body, err := json.Marshal(&msg)
	if err != nil {
		log.Fatal(4, "failed to marshal persistMessage to json.")
	}
	// This will always return a host. If all hosts are currently marked as dead,
	// then all hosts will be reset to alive and we will try them all again. This
	// will result in this loop repeating forever until we successfully publish our msg.
	hostPoolResponse := hostPool.Get()
	p := producers[hostPoolResponse.Host()]
	err = p.Publish(*topicNotifyPersist, body)
	// Hosts that are marked as dead will be retried after 30seconds.  If we published
	// successfully, then sending a nil error will mark the host as alive again.
	hostPoolResponse.Mark(err)
	if err != nil {
		log.Warn("publisher marking host %s as faulty due to %s", hostPoolResponse.Host(), err)
		msg.Send()
	}

	return
}

type MetricPersistHandler struct {
	metrics Metrics
}

func NewMetricPersistHandler(metrics Metrics) *MetricPersistHandler {
	return &MetricPersistHandler{
		metrics: metrics,
	}
}

func (h *MetricPersistHandler) HandleMessage(m *nsq.Message) error {
	ms := PersistMessage{}
	err := json.Unmarshal(m.Body, &ms)
	if err != nil {
		log.Error(3, "skipping message. %s", err)
		return nil
	}
	if ms.Instance == *instance {
		log.Debug("skipping message we generated. %s - %s:%d", ms.Instance, ms.Key, ms.T0)
		return nil
	}

	// get metric
	if agg, ok := metrics.Get(ms.Key); ok {
		// Sync the save state of our chunk for the specific T0 referenced in the msg.
		agg.(*AggMetric).SyncChunkSaveState(ms.T0)
	}

	return nil
}

func InitCluster(instance string, initialState bool, metrics Metrics, stats met.Backend) {
	clusterStatus = NewClusterStatus(instance, initialState)

	// init producers
	pCfg := nsq.NewConfig()
	pCfg.UserAgent = "metrics_tank"
	err := app.ParseOpts(pCfg, *producerOpts)
	if err != nil {
		log.Fatal(4, "failed to parse nsq producer options. %s", err)
	}
	hostPool = hostpool.NewEpsilonGreedy(strings.Split(*nsqdTCPAddrs, ","), 0, &hostpool.LinearEpsilonValueCalculator{})
	producers = make(map[string]*nsq.Producer)

	for _, addr := range strings.Split(*nsqdTCPAddrs, ",") {
		producer, err := nsq.NewProducer(addr, pCfg)
		if err != nil {
			log.Fatal(4, "failed creating producer %s", err.Error())
		}
		producers[addr] = producer
	}

	// init consumers
	cfg := nsq.NewConfig()
	cfg.UserAgent = "metrics_tank_cluster"
	err = app.ParseOpts(cfg, *consumerOpts)
	if err != nil {
		log.Fatal(4, "failed to parse nsq consumer options. %s", err)
	}
	cfg.MaxInFlight = *maxInFlight

	consumer, err := insq.NewConsumer(*topicNotifyPersist, fmt.Sprintf("metric_tank_%s", instance), cfg, "metric_persist.%s", stats)
	if err != nil {
		log.Fatal(4, "Failed to create NSQ consumer. %s", err)
	}
	handler := NewMetricPersistHandler(metrics)
	consumer.AddHandler(handler)

	nsqdAdds := strings.Split(*nsqdTCPAddrs, ",")
	if len(nsqdAdds) == 1 && nsqdAdds[0] == "" {
		nsqdAdds = []string{}
	}
	err = consumer.ConnectToNSQDs(nsqdAdds)
	if err != nil {
		log.Fatal(4, "failed to connect to NSQDs. %s", err)
	}
	log.Info("persist consumer connected to nsqd")

	lookupdAdds := strings.Split(*lookupdHTTPAddrs, ",")
	if len(lookupdAdds) == 1 && lookupdAdds[0] == "" {
		lookupdAdds = []string{}
	}
	err = consumer.ConnectToNSQLookupds(lookupdAdds)
	if err != nil {
		log.Fatal(4, "failed to connect to NSQLookupds. %s", err)
	}
}

// Handlers for HTTP interface.
// Handle requests for /cluster. POST to set primary flag, GET to get current state.
func clusterStatusHandler(w http.ResponseWriter, req *http.Request) {
	if req.Method == "GET" {
		getClusterStatus(w, req)
		return
	}
	if req.Method == "POST" {
		setClusterStatus(w, req)
		return
	}
	http.Error(w, "not found.", http.StatusNotFound)
}

func setClusterStatus(w http.ResponseWriter, req *http.Request) {
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
	clusterStatus.Set(primary)
	log.Info("primary status is now %t", primary)
	w.Write([]byte("OK"))
}

func getClusterStatus(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	resp, err := clusterStatus.Marshal()
	if err != nil {
		http.Error(w, "could not marshal status to json", http.StatusInternalServerError)
		return
	}
	w.Write(resp)
}
