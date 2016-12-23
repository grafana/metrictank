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
	"github.com/raintank/metrictank/cassandra"
	"github.com/raintank/metrictank/cluster"
	"github.com/raintank/metrictank/idx"
	"github.com/raintank/metrictank/idx/memory"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"
	"gopkg.in/raintank/schema.v1"
)

const KeyspaceSchema = `CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}  AND durable_writes = true`
const TableSchema = `CREATE TABLE IF NOT EXISTS %s.metric_idx (
    id text,
    orgid int,
    partition int,
    name text,
    metric text,
    interval int,
    unit text,
    mtype text,
    tags set<text>,
    lastupdate int,
    PRIMARY KEY (id, partition)
) WITH compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}`
const MetricIdxPartitionIndex = `CREATE INDEX IF NOT EXISTS ON %s.metric_idx(partition)`

var (
	idxCasOk             met.Count // metric idx.cassadra.ok is how many metrics are successfully being indexed
	idxCasFail           met.Count // metric idx.cassandra.fail is how failures encountered while trying to index metrics
	idxCasAddDuration    met.Timer
	idxCasDeleteDuration met.Timer
	metrics              cassandra.Metrics

	Enabled          bool
	ssl              bool
	auth             bool
	hostverification bool
	keyspace         string
	hosts            string
	capath           string
	username         string
	password         string
	consistency      string
	timeout          time.Duration
	numConns         int
	writeQueueSize   int
	protoVer         int
	maxStale         time.Duration
	pruneInterval    time.Duration
	updateInterval   time.Duration
	updateFuzzyness  float64
)

func ConfigSetup() {
	casIdx := flag.NewFlagSet("cassandra-idx", flag.ExitOnError)

	casIdx.BoolVar(&Enabled, "enabled", false, "")
	casIdx.StringVar(&hosts, "hosts", "localhost:9042", "comma separated list of cassandra addresses in host:port form")
	casIdx.StringVar(&keyspace, "keyspace", "metric", "Cassandra keyspace to store metricDefinitions in.")
	casIdx.StringVar(&consistency, "consistency", "one", "write consistency (any|one|two|three|quorum|all|local_quorum|each_quorum|local_one")
	casIdx.DurationVar(&timeout, "timeout", time.Second, "cassandra request timeout")
	casIdx.IntVar(&numConns, "num-conns", 10, "number of concurrent connections to cassandra")
	casIdx.IntVar(&writeQueueSize, "write-queue-size", 100000, "Max number of metricDefs allowed to be unwritten to cassandra")
	casIdx.DurationVar(&updateInterval, "update-interval", time.Hour*3, "frequency at which we should update the metricDef lastUpdate field.")
	casIdx.Float64Var(&updateFuzzyness, "update-fuzzyness", 0.5, "fuzzyness factor for update-interval. should be in the range 0 > fuzzyness <= 1. With an updateInterval of 4hours and fuzzyness of 0.5, metricDefs will be updated every 4-6hours.")
	casIdx.DurationVar(&maxStale, "max-stale", 0, "clear series from the index if they have not been seen for this much time.")
	casIdx.DurationVar(&pruneInterval, "prune-interval", time.Hour*3, "Interval at which the index should be checked for stale series.")
	casIdx.IntVar(&protoVer, "protocol-version", 4, "cql protocol version to use")

	casIdx.BoolVar(&ssl, "ssl", false, "enable SSL connection to cassandra")
	casIdx.StringVar(&capath, "ca-path", "/etc/raintank/ca.pem", "cassandra CA certficate path when using SSL")
	casIdx.BoolVar(&hostverification, "host-verification", true, "host (hostname and server cert) verification when using SSL")

	casIdx.BoolVar(&auth, "auth", false, "enable cassandra user authentication")
	casIdx.StringVar(&username, "username", "cassandra", "username for authentication")
	casIdx.StringVar(&password, "password", "cassandra", "password for authentication")

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
	if ssl {
		cluster.SslOpts = &gocql.SslOptions{
			CaPath:                 capath,
			EnableHostVerification: hostverification,
		}
	}
	if auth {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: username,
			Password: password,
		}
	}

	return &CasIdx{
		MemoryIdx:  *memory.New(),
		cluster:    cluster,
		writeQueue: make(chan writeReq, writeQueueSize),
		shutdown:   make(chan struct{}),
	}
}

func (c *CasIdx) Init(stats met.Backend) error {
	log.Info("initializing cassandra-idx. Hosts=%s", hosts)
	if err := c.MemoryIdx.Init(stats); err != nil {
		return err
	}

	var err error
	tmpSession, err := c.cluster.CreateSession()
	if err != nil {
		log.Error(3, "cassandra-idx failed to create cassandra session. %s", err)
		return err
	}

	// ensure the keyspace and table exist.
	err = tmpSession.Query(fmt.Sprintf(KeyspaceSchema, keyspace)).Exec()
	if err != nil {
		log.Error(3, "cassandra-idx failed to initialize cassandra keyspace. %s", err)
		return err
	}
	err = tmpSession.Query(fmt.Sprintf(TableSchema, keyspace)).Exec()
	if err != nil {
		log.Error(3, "cassandra-idx failed to initialize cassandra table. %s", err)
		return err
	}
	err = tmpSession.Query(fmt.Sprintf(MetricIdxPartitionIndex, keyspace)).Exec()
	if err != nil {
		log.Error(3, "cassandra-idx failed to initialize cassandra index. %s", err)
		return err
	}
	tmpSession.Close()
	c.cluster.Keyspace = keyspace
	session, err := c.cluster.CreateSession()
	if err != nil {
		log.Error(3, "cassandra-idx failed to create cassandra session. %s", err)
		return err
	}

	c.session = session

	idxCasOk = stats.NewCount("idx.cassandra.ok")
	idxCasFail = stats.NewCount("idx.cassandra.fail")
	idxCasAddDuration = stats.NewTimer("idx.cassandra.add_duration", 0)
	idxCasDeleteDuration = stats.NewTimer("idx.cassandra.delete_duration", 0)
	metrics = cassandra.NewMetrics("idx.cassandra", stats)

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
	log.Info("cassandra-idx stopping")
	c.MemoryIdx.Stop()
	close(c.writeQueue)
	c.wg.Wait()
	c.session.Close()
}

func (c *CasIdx) Add(data *schema.MetricData, partition int32) error {
	existing, err := c.MemoryIdx.Get(data.Id)
	inMemory := true
	if err != nil {
		if err == idx.DefNotFound {
			inMemory = false
		} else {
			log.Error(3, "cassandra-idx Failed to query Memory Index for %s. %s", data.Id, err)
			return err
		}
	}

	if inMemory && existing.Partition == partition {
		oldest := time.Now().Add(-1 * updateInterval).Add(-1 * time.Duration(rand.Int63n(updateInterval.Nanoseconds()*int64(updateFuzzyness*100))))
		if existing.LastUpdate < oldest.Unix() {
			log.Debug("cassandra-idx def hasnt been seem for a while, updating index.")
			def := schema.MetricDefinitionFromMetricData(data)
			def.Partition = partition
			c.MemoryIdx.AddDef(def)
			c.writeQueue <- writeReq{recvTime: time.Now(), def: def}
		}
		return nil
	}
	def := schema.MetricDefinitionFromMetricData(data)
	def.Partition = partition
	err = c.MemoryIdx.AddDef(def)
	if err == nil {
		c.writeQueue <- writeReq{recvTime: time.Now(), def: def}
	}
	return err
}

func (c *CasIdx) rebuildIndex() {
	log.Info("cassandra-idx Rebuilding Memory Index from metricDefinitions in Cassandra")
	pre := time.Now()
	defs := make([]schema.MetricDefinition, 0)
	for _, partition := range cluster.ThisNode.GetPartitions() {
		iter := c.session.Query("SELECT id, orgid, partition, name, metric, interval, unit, mtype, tags, lastupdate from metric_idx where partition=?", partition).Iter()

		mdef := schema.MetricDefinition{}
		var id, name, metric, unit, mtype string
		var orgId int
		var partition int32
		var lastupdate int64
		var interval int
		var tags []string

		for iter.Scan(&id, &orgId, &partition, &name, &metric, &interval, &unit, &mtype, &tags, &lastupdate) {
			mdef.Id = id
			mdef.OrgId = orgId
			mdef.Partition = partition
			mdef.Name = name
			mdef.Metric = metric
			mdef.Interval = interval
			mdef.Unit = unit
			mdef.Mtype = mtype
			mdef.Tags = tags
			mdef.LastUpdate = lastupdate
			defs = append(defs, mdef)
		}
		if err := iter.Close(); err != nil {
			log.Fatal(4, "Could not close iterator: %s", err.Error())
		}
	}
	c.MemoryIdx.Load(defs)
	log.Info("Rebuilding Memory Index Complete. Took %s to load %d series", time.Since(pre).String(), len(defs))
}

func (c *CasIdx) processWriteQueue() {
	log.Info("cassandra-idx writeQueue handler started.")
	var success bool
	var attempts int
	var err error
	var req writeReq
	qry := `INSERT INTO metric_idx (id, orgid, partition, name, metric, interval, unit, mtype, tags, lastupdate) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	for req = range c.writeQueue {
		if err != nil {
			log.Error(3, "Failed to marshal metricDef. %s", err)
			continue
		}
		success = false
		attempts = 0

		for !success {
			if err := c.session.Query(
				qry,
				req.def.Id,
				req.def.OrgId,
				req.def.Partition,
				req.def.Name,
				req.def.Metric,
				req.def.Interval,
				req.def.Unit,
				req.def.Mtype,
				req.def.Tags,
				req.def.LastUpdate).Exec(); err != nil {

				idxCasFail.Inc(1)
				metrics.Inc(err)
				if (attempts % 20) == 0 {
					log.Warn("cassandra-idx Failed to write def to cassandra. it will be retried. %s", err)
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
				log.Debug("cassandra-idx metricDef saved to cassandra. %s", req.def.Id)
			}
		}
	}
	log.Info("cassandra-idx writeQueue handler ended.")
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
			cErr := c.session.Query("DELETE FROM metric_idx where id=?", def.Id).Exec()
			if cErr != nil {
				metrics.Inc(err)
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
			cErr := c.session.Query("DELETE FROM metric_idx where id=?", def.Id).Exec()
			if cErr != nil {
				metrics.Inc(err)
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
