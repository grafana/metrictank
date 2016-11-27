package elasticsearch

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/mattbaird/elastigo/lib"
	"github.com/raintank/metrictank/cluster"
	"github.com/raintank/metrictank/idx"
	"github.com/raintank/metrictank/idx/memory"
	"github.com/raintank/metrictank/stats"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"
	"gopkg.in/raintank/schema.v1"
)

var (
	idxEsOk             = stats.NewCounter32("idx.elasticsearch.ok")
	idxEsFail           = stats.NewCounter32("idx.elasticsearch.fail")
	idxEsAddDuration    = stats.NewLatencyHistogram15s32("idx.elasticsearch.add_duration")
	idxEsDeleteDuration = stats.NewLatencyHistogram15s32("idx.elasticsearch.delete_duration")
	retryBufItems       = stats.NewGauge32("idx.elasticsearch.retrybuf.items")

	Enabled          bool
	esIndex          string
	esHosts          string
	esUser           string
	esPass           string
	esRetryInterval  time.Duration
	esMaxConns       int
	esMaxBufferDocs  int
	esBufferDelayMax time.Duration
)

func ConfigSetup() {
	esIdx := flag.NewFlagSet("elasticsearch-idx", flag.ExitOnError)
	esIdx.BoolVar(&Enabled, "enabled", false, "")
	esIdx.StringVar(&esIndex, "index", "metric", "Elasticsearch index name for storing metric index.")
	esIdx.StringVar(&esHosts, "hosts", "localhost:9200", "comma separated list of elasticsearch address in host:port form")
	esIdx.StringVar(&esUser, "user", "", "HTTP basic Auth username")
	esIdx.StringVar(&esPass, "pass", "", "HTTP basic Auth password")
	esIdx.DurationVar(&esRetryInterval, "retry-interval", time.Minute*10, "Interval to retry indexing Definitions in ES after failures")
	esIdx.IntVar(&esMaxConns, "max-conns", 20, "Max number of http conns in flight to ES servers at one time")
	esIdx.IntVar(&esMaxBufferDocs, "max-buffer-docs", 1000, "Max number of Docs to hold in buffer before forcing flush to ES")
	esIdx.DurationVar(&esBufferDelayMax, "buffer-delay-max", time.Second*10, "Max time to wait before flusing forcing flush to ES")
	globalconf.Register("elasticsearch-idx", esIdx)
}

type RetryBuffer struct {
	Index       *EsIdx
	Defs        []schema.MetricDefinition
	LastAttempt time.Time
	done        chan struct{}
	sync.Mutex
}

func NewRetryBuffer(index *EsIdx, interval time.Duration) *RetryBuffer {
	r := &RetryBuffer{
		Index:       index,
		Defs:        make([]schema.MetricDefinition, 0),
		LastAttempt: time.Now(),
		done:        make(chan struct{}),
	}
	go r.run(interval)
	return r
}

func (r *RetryBuffer) Stop() {
	close(r.done)
}

func (r *RetryBuffer) Items() []schema.MetricDefinition {
	r.Lock()
	defs := make([]schema.MetricDefinition, len(r.Defs))
	copy(defs, r.Defs)
	r.Unlock()
	return defs
}

// called when failing to bulkindexer fails, or when got a failure back from ES
func (r *RetryBuffer) Queue(id string) {
	def, err := r.Index.Get(id)
	if err != nil {
		log.Error(3, "Failed to get %s from Memory Index. %s", id, err)
		return
	}
	r.Lock()
	r.Defs = append(r.Defs, def)
	retryBufItems.Set(len(r.Defs))
	r.Unlock()
}

func (r *RetryBuffer) retry() {
	r.Lock()
	defs := r.Defs
	r.Defs = make([]schema.MetricDefinition, 0, len(defs))
	retryBufItems.Set(0)
	r.Unlock()
	if len(defs) == 0 {
		log.Debug("retry buffer is empty")
		return
	}
	for _, d := range defs {
		if err := r.Index.BulkIndexer.Index(esIndex, "metric_index", d.Id, "", "", nil, d); err != nil {
			log.Error(3, "Failed to add metricDef to BulkIndexer queue. %s", err)
			r.Defs = append(r.Defs, d)
			retryBufItems.Set(len(r.Defs))
			return
		}
	}
}

func (r *RetryBuffer) run(interval time.Duration) {
	log.Debug("Starting RetryBuffer")
	ticker := time.NewTicker(interval)
	for {
		select {
		case <-r.done:
			r.Lock()
			log.Info("RetryBuffer stopping. %d items in cache have not been indexed.", len(r.Defs))
			ticker.Stop()
			r.Unlock()
			return
		case <-ticker.C:
			r.retry()
		}
	}
}

// Implements the the "MetricIndex" interface
type EsIdx struct {
	memory.MemoryIdx
	Conn        *elastigo.Conn
	BulkIndexer *elastigo.BulkIndexer
	retryBuf    *RetryBuffer
	mu          sync.Mutex
}

func New() *EsIdx {
	conn := elastigo.NewConn()
	conn.SetHosts(strings.Split(esHosts, ","))
	if esUser != "" {
		conn.Username = esUser
	}
	if esPass != "" {
		conn.Password = esPass
	}

	return &EsIdx{
		MemoryIdx: *memory.New(),
		Conn:      conn,
	}
}

func (e *EsIdx) Init() error {

	if esRetryInterval < time.Second {
		return errors.New("Invalid retry-interval.  Valid units are 's', 'm', 'h'. Must be at least 1 second")
	}

	log.Info("initializing EsIdx. Hosts=%s", esHosts)
	if err := e.MemoryIdx.Init(); err != nil {
		return err
	}
	log.Info("Checking if index %s exists in ES", esIndex)
	if exists, err := e.Conn.ExistsIndex(esIndex, "", nil); err != nil && err.Error() != "record not found" {
		return err
	} else {
		if !exists {
			log.Info("ES: initializing %s Index with mapping", esIndex)
			_, err = e.Conn.DoCommand("PUT", fmt.Sprintf("/%s", esIndex), nil, mapping)
			if err != nil {
				return err
			}
			// if we dont wait here, rebuildIndex will fail due to ES returing a 503.
			time.Sleep(time.Second)
		}
	}
	log.Info("Setting up ES bulkIndexer")
	e.BulkIndexer = e.Conn.NewBulkIndexer(esMaxConns)

	//dont retry sends.
	e.BulkIndexer.RetryForSeconds = 0

	// index at most MaxBufferDocs per request.
	e.BulkIndexer.BulkMaxDocs = esMaxBufferDocs
	//Setting BulkMaxBuffer to 1KB per doc ensure we will hold at least MaxBufferDocs before flushing
	e.BulkIndexer.BulkMaxBuffer = 1024 * esMaxBufferDocs

	//flush at least every esBufferDelayMax.
	e.BulkIndexer.BufferDelayMax = esBufferDelayMax
	e.BulkIndexer.Refresh = true
	e.BulkIndexer.Sender = e.bulkSend

	e.retryBuf = NewRetryBuffer(e, esRetryInterval)

	log.Info("Starting BulkIndexer")
	e.BulkIndexer.Start()

	//Rebuild the in-memory index.
	e.rebuildIndex()
	return nil
}

func (e *EsIdx) Add(data *schema.MetricData, partition int32) error {
	existing, err := e.MemoryIdx.Get(data.Id)
	inMemory := true
	if err != nil {
		if err == idx.DefNotFound {
			inMemory = false
		} else {
			log.Error(3, "Failed to query Memory Index for %s. %s", data.Id, err)
			return err
		}
	}
	def := schema.MetricDefinitionFromMetricData(data)
	def.Partition = partition
	if inMemory && existing.Partition == def.Partition {
		log.Debug("def already seen before. Just updating memory Index")
		e.MemoryIdx.AddDef(def)
		return nil
	}

	err = e.MemoryIdx.AddDef(def)
	if err == nil {
		if err = e.BulkIndexer.Index(esIndex, "metric_index", def.Id, "", "", nil, def); err != nil {
			log.Error(3, "Failed to add metricDef to BulkIndexer queue. %s", err)
			e.retryBuf.Queue(def.Id)
		}
	}
	return err
}

func (e *EsIdx) bulkSend(buf *bytes.Buffer) error {
	pre := time.Now()
	log.Debug("ES: sending defs batch")
	body, err := e.Conn.DoCommand("POST", fmt.Sprintf("/_bulk?refresh=%t", e.BulkIndexer.Refresh), nil, buf)

	// If something goes wrong at this stage, return an error and bulkIndexer will retry.
	if err != nil {
		log.Error(3, "ES: failed to send defs batch. will retry: %s", err)
		return err
	}

	if err := e.processEsResponse(body); err != nil {
		return err
	}
	idxEsAddDuration.Value(time.Since(pre))
	return nil
}

type responseStruct struct {
	Took   int64                    `json:"took"`
	Errors bool                     `json:"errors"`
	Items  []map[string]interface{} `json:"items"`
}

func (e *EsIdx) processEsResponse(body []byte) error {
	response := responseStruct{}

	// check for response errors, bulk insert will give 200 OK but then include errors in response
	err := json.Unmarshal(body, &response)
	if err != nil {
		// Something went *extremely* wrong trying to submit these items
		// to elasticsearch. return an error and bulkIndexer will retry.
		log.Error(3, "ES: bulkindex response parse failed: %q", err)
		return err
	}
	if response.Errors {
		log.Warn("ES: Bulk Insertion: some operations failed, they will be retried.")
	} else {
		log.Debug("ES: Bulk Insertion: all operations succeeded")
		docCount := 0
		for _, m := range response.Items {
			docCount += len(m)
		}
		idxEsOk.Add(docCount)
		return nil
	}

	for _, m := range response.Items {
		for _, v := range m {
			v := v.(map[string]interface{})
			id := v["_id"].(string)
			if errStr, ok := v["error"].(string); ok {
				log.Warn("ES: %s failed: %s", id, errStr)
				e.retryBuf.Queue(id)
				idxEsFail.Inc()
			} else if errMap, ok := v["error"].(map[string]interface{}); ok {
				log.Warn("ES: %s failed: %s: %q", id, errMap["type"].(string), errMap["reason"].(string))
				e.retryBuf.Queue(id)
				idxEsFail.Inc()
			} else {
				log.Debug("ES: completed %s successfully.", id)
				idxEsOk.Inc()
			}
		}
	}
	return nil
}

func (e *EsIdx) Stop() {
	log.Info("stopping ES Index")
	e.MemoryIdx.Stop()
	e.BulkIndexer.Stop()
	e.retryBuf.Stop()
}

func (e *EsIdx) rebuildIndex() {
	log.Info("Rebuilding Memory Index from metricDefinitions in ES")
	pre := time.Now()
	defs := make([]schema.MetricDefinition, 0)
	var err error
	var out elastigo.SearchResult
	qry := map[string]map[string]map[string]int32{
		"query": map[string]map[string]int32{
			"term": map[string]int32{"partition": 0},
		},
	}
	for _, partition := range cluster.ThisNode.GetPartitions() {
		qry["query"]["term"]["partition"] = partition
		loading := true
		scroll_id := ""
		for loading {
			if scroll_id == "" {
				out, err = e.Conn.Search(esIndex, "metric_index", map[string]interface{}{"scroll": "1m", "size": 1000}, qry)
			} else {
				out, err = e.Conn.Scroll(map[string]interface{}{"scroll": "1m"}, scroll_id)
			}
			if err != nil {
				log.Fatal(4, "Failed to load metric definitions from ES. %s", err)
			}
			for _, h := range out.Hits.Hits {
				mdef, err := schema.MetricDefinitionFromJSON(*h.Source)
				if err != nil {
					log.Error(3, "Bad definition in index. %s - %s", h.Source, err)
				}
				defs = append(defs, *mdef)
			}

			scroll_id = out.ScrollId
			if out.Hits.Len() == 0 {
				loading = false
			}
		}
	}
	e.MemoryIdx.Load(defs)
	log.Info("Rebuilding Memory Index Complete. Took %s", time.Since(pre).String())
}

func (e *EsIdx) Delete(orgId int, pattern string) ([]schema.MetricDefinition, error) {
	defs, err := e.MemoryIdx.Delete(orgId, pattern)
	if err != nil {
		return defs, err
	}
	for _, def := range defs {
		e.BulkIndexer.Delete(esIndex, "metric_index", def.Id)
	}
	return defs, nil
}

func (e *EsIdx) Prune(orgId int, oldest time.Time) ([]schema.MetricDefinition, error) {
	pruned, err := e.MemoryIdx.Prune(orgId, oldest)
	// if an error was encountered then pruned is probably a partial list of metricDefs
	// deleted, so lets still try and delete these from Cassandra.
	for _, def := range pruned {
		e.BulkIndexer.Delete(esIndex, "metric_index", def.Id)
	}
	return pruned, err
}
