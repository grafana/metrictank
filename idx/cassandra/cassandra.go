package cassandra

import (
	"flag"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/raintank/met"
	"github.com/raintank/metrictank/cluster"
	"github.com/raintank/metrictank/idx"
	"github.com/raintank/metrictank/idx/memory"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"
	"gopkg.in/raintank/schema.v1"
)

const keyspace_schema = `CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}  AND durable_writes = true`
const table_schema = `CREATE TABLE IF NOT EXISTS %s.metric_def_idx (
    id text PRIMARY KEY,
    def blob,
) WITH compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}`

var (
	idxCasOk             met.Count
	idxCasFail           met.Count
	idxCasAddDuration    met.Timer
	idxCasDeleteDuration met.Timer

	Enabled         bool
	keyspace        string
	hosts           string
	consistency     string
	timeout         time.Duration
	numConns        int
	writeQueueSize  int
	protoVer        int
	maxStale        time.Duration
	pruneInterval   time.Duration
	updateInterval  time.Duration
	updateFuzzyness float64
)

func ConfigSetup() {
	casIdx := flag.NewFlagSet("cassandra-idx", flag.ExitOnError)
	casIdx.BoolVar(&Enabled, "enabled", false, "")
	casIdx.StringVar(&keyspace, "keyspace", "metric", "Cassandra keyspace to store metricDefinitions in.")
	casIdx.StringVar(&hosts, "hosts", "localhost:9042", "comma separated list of cassandra addresses in host:port form")
	casIdx.StringVar(&consistency, "consistency", "one", "write consistency (any|one|two|three|quorum|all|local_quorum|each_quorum|local_one")
	casIdx.DurationVar(&timeout, "timeout", time.Second, "cassandra request timeout")
	casIdx.IntVar(&numConns, "num-conns", 10, "number of concurrent connections to cassandra")
	casIdx.IntVar(&writeQueueSize, "write-queue-size", 100000, "Max number of metricDefs allowed to be unwritten to cassandra")
	casIdx.IntVar(&protoVer, "protocol-version", 4, "cql protocol version to use")
	casIdx.DurationVar(&updateInterval, "update-interval", time.Hour*3, "frequency at which we should update the metricDef lastUpdate field.")
	casIdx.Float64Var(&updateFuzzyness, "update-fuzzyness", 0.5, "fuzzyness factor for update-interval. should be in the range 0 > fuzzyness <= 1. With an updateInterval of 4hours and fuzzyness of 0.5, metricDefs will be updated every 4-6hours.")
	casIdx.DurationVar(&maxStale, "max-stale", 0, "clear series from the index if they have not been seen for this much time.")
	casIdx.DurationVar(&pruneInterval, "prune-interval", time.Hour*3, "Interval at which the index should be checked for stale series.")
	globalconf.Register("cassandra-idx", casIdx)
}

type writeReq struct {
	def      *schema.MetricDefinition
	recvTime time.Time
}

// Implements the the "MetricIndex" interface
type CasIdx struct {
	memory.MemoryIdx
	cluster    *gocql.ClusterConfig
	session    *gocql.Session
	writeQueue chan writeReq
	shutdown   chan struct{}
	wg         sync.WaitGroup
	initialize bool
}

func New(initialize bool) *CasIdx {
	cluster := gocql.NewCluster(strings.Split(hosts, ",")...)
	cluster.Consistency = gocql.ParseConsistency(consistency)
	cluster.Timeout = timeout
	cluster.NumConns = numConns
	cluster.ProtoVersion = protoVer

	return &CasIdx{
		MemoryIdx:  *memory.New(),
		cluster:    cluster,
		writeQueue: make(chan writeReq, writeQueueSize),
		shutdown:   make(chan struct{}),
		initialize: initialize,
	}
}

func (c *CasIdx) Init(stats met.Backend) error {
	log.Info("IDX-C Initializing. Hosts=%s", hosts)
	if err := c.MemoryIdx.Init(stats); err != nil {
		return err
	}

	var err error

	if c.initialize {
		tmpSession, err := c.cluster.CreateSession()
		if err != nil {
			log.Error(3, "IDX-C failed to create cassandra session. %s", err)
			return err
		}

		// ensure the keyspace and table exist.
		err = tmpSession.Query(fmt.Sprintf(keyspace_schema, keyspace)).Exec()
		if err != nil {
			log.Error(3, "IDX-C failed to initialize cassandra keyspace. %s", err)
			return err
		}
		err = tmpSession.Query(fmt.Sprintf(table_schema, keyspace)).Exec()
		if err != nil {
			log.Error(3, "IDX-C failed to initialize cassandra table. %s", err)
			return err
		}
		tmpSession.Close()
	}
	c.cluster.Keyspace = keyspace
	session, err := c.cluster.CreateSession()
	if err != nil {
		log.Error(3, "IDX-C failed to create cassandra session. %s", err)
		return err
	}

	c.session = session

	idxCasOk = stats.NewCount("idx.cassandra.ok")
	idxCasFail = stats.NewCount("idx.cassandra.fail")
	idxCasAddDuration = stats.NewTimer("idx.cassandra.add_duration", 0)
	idxCasDeleteDuration = stats.NewTimer("idx.cassandra.delete_duration", 0)

	for i := 0; i < numConns; i++ {
		c.wg.Add(1)
		go c.processWriteQueue()
	}
	//Rebuild the in-memory index.

	c.rebuildIndex()
	if maxStale > 0 {
		if pruneInterval == 0 {
			return fmt.Errorf("pruneInterval must be greater then 0")
		}
		go c.prune()
	}
	return nil
}

func (c *CasIdx) Stop() {
	log.Info("IDX-C Stopping")
	c.MemoryIdx.Stop()
	close(c.writeQueue)
	c.wg.Wait()
	c.session.Close()
}

func (c *CasIdx) Add(data *schema.MetricData) {
	existing, err := c.MemoryIdx.Get(data.Id)
	inMemory := true
	if err != nil {
		if err == idx.DefNotFound {
			inMemory = false
		} else {
			log.Error(3, "IDX-C Failed to query Memory Index for %s. %s", data.Id, err)
			return
		}
	}
	if inMemory {
		oldest := time.Now().Add(-1 * updateInterval).Add(-1 * time.Duration(rand.Int63n(updateInterval.Nanoseconds()*int64(updateFuzzyness*100))))
		if existing.LastUpdate < oldest.Unix() {
			log.Debug("cassandra-idx def hasnt been seem for a while, updating index.")
			existing.LastUpdate = data.Time
			c.MemoryIdx.AddDef(&existing)
			c.writeQueue <- writeReq{recvTime: time.Now(), def: &existing}
		}
		return
	}
	def := schema.MetricDefinitionFromMetricData(data)
	c.MemoryIdx.AddDef(def)
	c.writeQueue <- writeReq{recvTime: time.Now(), def: def}
}

func (c *CasIdx) rebuildIndex() {
	log.Info("IDX-C Rebuilding Memory Index from metricDefinitions in Cassandra")
	pre := time.Now()
	partitionCount := cluster.ThisCluster.GetPartitionCount()
	partitioner := cluster.ThisCluster.GetPartitioner()
	activePartitions := make(map[int32]struct{})
	for _, part := range cluster.ThisNode.GetPartitions() {
		activePartitions[part] = struct{}{}
	}
	defs := make([]schema.MetricDefinition, 0)
	iter := c.session.Query("SELECT def from metric_def_idx").Iter()

	var data []byte
	mdef := schema.MetricDefinition{}
	for iter.Scan(&data) {
		_, err := mdef.UnmarshalMsg(data)
		if err != nil {
			log.Error(3, "IDX-C Bad definition in index. %s - %s", data, err)
			continue
		}
		// only index metrics that match the partitions this node is handling.
		part := partitioner.GetPartition(&mdef, partitionCount)
		if _, ok := activePartitions[part]; ok {
			defs = append(defs, mdef)
		}
	}
	c.MemoryIdx.Load(defs)
	log.Info("IDX-C Rebuilding Memory Index Complete. Took %s to load %d defs", time.Since(pre).String(), len(defs))
}

func (c *CasIdx) processWriteQueue() {
	log.Info("IDX-C writeQueue handler started.")
	data := make([]byte, 0)
	var success bool
	var attempts int
	var err error
	var req writeReq
	for req = range c.writeQueue {
		data = data[:0]
		data, err = req.def.MarshalMsg(data)
		if err != nil {
			log.Error(3, "IDX-C Failed to marshal metricDef. %s", err)
			continue
		}
		success = false
		attempts = 0
		for !success {
			if err := c.session.Query(`INSERT INTO metric_def_idx (id, def) VALUES (?, ?)`, req.def.Id, data).Exec(); err != nil {
				idxCasFail.Inc(1)
				if (attempts % 20) == 0 {
					log.Warn("IDX-C Failed to write def to cassandra. it will be retried. %s", err)
				}
				sleepTime := 100 * attempts
				if sleepTime > 2000 {
					sleepTime = 2000
				}
				time.Sleep(time.Duration(sleepTime) * time.Millisecond)
				attempts++
			} else {
				success = true
				idxCasAddDuration.Value(time.Since(req.recvTime))
				idxCasOk.Inc(1)
				log.Debug("IDX-C metricDef saved to cassandra. %s", req.def.Id)
			}
		}
	}
	log.Info("IDX-C writeQueue handler ended.")
	c.wg.Done()
}

func (c *CasIdx) Delete(orgId int, pattern string) ([]schema.MetricDefinition, error) {
	defs, err := c.MemoryIdx.Delete(orgId, pattern)
	if err != nil {
		return defs, err
	}
	for _, def := range defs {
		attempts := 0
		deleted := false
		for !deleted && attempts < 5 {
			attempts++
			cErr := c.session.Query("DELETE FROM metric_def_idx where id=?", def.Id).Exec()
			if cErr != nil {
				log.Error(3, "cassandra-idx Failed to delete metricDef %s from cassandra. %s", def.Id, err)
				time.Sleep(time.Second)
			} else {
				deleted = true
			}
		}
	}
	return defs, nil
}

func (c *CasIdx) Prune(orgId int, oldest time.Time) ([]schema.MetricDefinition, error) {
	pruned, err := c.MemoryIdx.Prune(orgId, oldest)
	// if an error was encountered then pruned is probably a partial list of metricDefs
	// deleted, so lets still try and delete these from Cassandra.
	for _, def := range pruned {
		log.Debug("cassandra-idx: metricDef %s pruned from the index.", def.Id)
		attempts := 0
		deleted := false
		for !deleted && attempts < 5 {
			attempts++
			cErr := c.session.Query("DELETE FROM metric_def_idx where id=?", def.Id).Exec()
			if cErr != nil {
				log.Error(3, "cassandra-idx Failed to delete metricDef %s from cassandra. %s", def.Id, err)
				time.Sleep(time.Second)
			} else {
				deleted = true
			}
		}
	}
	return pruned, err
}

func (c *CasIdx) prune() {
	ticker := time.NewTicker(pruneInterval)
	for range ticker.C {
		log.Debug("cassandra-idx: pruning items from index that have not been seen for %s", maxStale.String())
		staleTs := time.Now().Add(maxStale * -1)
		_, err := c.Prune(-1, staleTs)
		if err != nil {
			log.Error(3, "cassandra-idx: prune error. %s", err)
		}
	}
}
