package cassandra

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/raintank/met"
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

	Enabled        bool
	keyspace       string
	hosts          string
	consistency    string
	timeout        time.Duration
	numConns       int
	writeQueueSize int
	protoVer       int
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
}

func New() *CasIdx {
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
	}
}

func (c *CasIdx) Init(stats met.Backend) error {
	log.Info("initializing CasIdx. Hosts=%s", hosts)
	if err := c.MemoryIdx.Init(stats); err != nil {
		return err
	}

	var err error
	tmpSession, err := c.cluster.CreateSession()
	if err != nil {
		log.Error(3, "failed to create cassandra session. %s", err)
		return err
	}

	// ensure the keyspace and table exist.
	err = tmpSession.Query(fmt.Sprintf(keyspace_schema, keyspace)).Exec()
	if err != nil {
		log.Error(3, "failed to initialize cassandra keyspace. %s", err)
		return err
	}
	err = tmpSession.Query(fmt.Sprintf(table_schema, keyspace)).Exec()
	if err != nil {
		log.Error(3, "failed to initialize cassandra table. %s", err)
		return err
	}
	tmpSession.Close()
	c.cluster.Keyspace = keyspace
	session, err := c.cluster.CreateSession()
	if err != nil {
		log.Error(3, "failed to create cassandra session. %s", err)
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
	return nil
}

func (c *CasIdx) Stop() {
	log.Info("stopping Cassandra-idx")
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
			log.Error(3, "Failed to query Memory Index for %s. %s", data.Id, err)
			return
		}
	}
	if inMemory {
		log.Debug("def already seen before. Just updating memory Index")
		existing.LastUpdate = data.Time
		c.MemoryIdx.AddDef(&existing)
		return
	}
	def := schema.MetricDefinitionFromMetricData(data)
	c.MemoryIdx.AddDef(def)
	c.writeQueue <- writeReq{recvTime: time.Now(), def: def}
}

func (c *CasIdx) rebuildIndex() {
	log.Info("Rebuilding Memory Index from metricDefinitions in Cassandra")
	pre := time.Now()
	defs := make([]schema.MetricDefinition, 0)
	iter := c.session.Query("SELECT def from metric_def_idx").Iter()

	var data []byte
	for iter.Scan(&data) {
		mdef, err := schema.MetricDefinitionFromJSON(data)
		if err != nil {
			log.Error(3, "Bad definition in index. %s - %s", data, err)
		}
		defs = append(defs, *mdef)
	}
	c.MemoryIdx.Load(defs)
	log.Info("Rebuilding Memory Index Complete. Took %s", time.Since(pre).String())
}

func (c *CasIdx) processWriteQueue() {
	log.Info("cassandra-idx writeQueue handler started.")
	var data bytes.Buffer
	encoder := json.NewEncoder(&data)
	var success bool
	var attempts int
	var req writeReq
	for req = range c.writeQueue {
		data.Reset()
		err := encoder.Encode(req.def)
		if err != nil {
			log.Error(3, "Failed to Unmarshal metricDef. %s", err)
			continue
		}
		success = false
		attempts = 0
		for !success {
			if err := c.session.Query(`INSERT INTO metric_def_idx (id, def) VALUES (?, ?)`, req.def.Id, data.Bytes()).Exec(); err != nil {
				idxCasFail.Inc(1)
				if (attempts % 20) == 0 {
					log.Warn("Failed to write def to cassandra. it will be retried. %s", err)
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
				log.Debug("metricDef saved to cassandra. %s", req.def.Id)
			}
		}
	}
	log.Info("cassandra-idx writeQueue handler ended.")
	c.wg.Done()
}

func (c *CasIdx) Delete(orgId int, pattern string) error {
	ids, err := c.MemoryIdx.DeleteWithReport(orgId, pattern)
	if err != nil {
		return err
	}
	for _, id := range ids {
		err := c.session.Query("DELETE FROM metric_def_idx where id=?", id).Exec()
		if err != nil {
			log.Error(3, "Failed to delete metricDef %s from cassandra. %s", id, err)
			return err
		}
	}
	return nil
}
