package cassandra

import (
	"flag"
	"fmt"
	"math/rand"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/raintank/metrictank/cassandra"
	"github.com/raintank/metrictank/cluster"
	"github.com/raintank/metrictank/idx/memory"
	"github.com/raintank/metrictank/stats"
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
    PRIMARY KEY (partition, id)
) WITH compaction = {'class': 'SizeTieredCompactionStrategy'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}`

var (
	// metric idx.cassadra.query-insert.ok is how many insert queries for a metric completed successfully (triggered by an add or an update)
	statQueryInsertOk = stats.NewCounter32("idx.cassandra.query-insert.ok")
	// metric idx.cassandra.query-insert.fail is how many insert queries for a metric failed (triggered by an add or an update)
	statQueryInsertFail = stats.NewCounter32("idx.cassandra.query-insert.fail")
	// metric idx.cassadra.query-delete.ok is how many delete queries for a metric completed successfully (triggered by an update or a delete)
	statQueryDeleteOk = stats.NewCounter32("idx.cassandra.query-delete.ok")
	// metric idx.cassandra.query-delete.fail is how many delete queries for a metric failed (triggered by an update or a delete)
	statQueryDeleteFail = stats.NewCounter32("idx.cassandra.query-delete.fail")

	// metric idx.cassandra.query-insert.wait is time inserts spent in queue before being executed
	statQueryInsertWaitDuration = stats.NewLatencyHistogram12h32("idx.cassandra.query-insert.wait")
	// metric idx.cassandra.query-insert.exec is time spent executing inserts (possibly repeatedly until success)
	statQueryInsertExecDuration = stats.NewLatencyHistogram15s32("idx.cassandra.query-insert.exec")
	// metric idx.cassandra.query-delete.exec is time spent executing deletes (possibly repeatedly until success)
	statQueryDeleteExecDuration = stats.NewLatencyHistogram15s32("idx.cassandra.query-delete.exec")

	// metric idx.cassandra.add is the duration of an add of one metric to the cassandra idx, including the add to the in-memory index, excluding the insert query
	statAddDuration = stats.NewLatencyHistogram15s32("idx.cassandra.add")
	// metric idx.cassandra.update is the duration of an update of one metric to the cassandra idx, including the update to the in-memory index, excluding any insert/delete queries
	statUpdateDuration = stats.NewLatencyHistogram15s32("idx.cassandra.update")
	// metric idx.cassandra.prune is the duration of a prune of the cassandra idx, including the prune of the in-memory index and all needed delete queries
	statPruneDuration = stats.NewLatencyHistogram15s32("idx.cassandra.prune")
	// metric idx.cassandra.delete is the duration of a delete of one or more metrics from the cassandra idx, including the delete from the in-memory index and the delete query
	statDeleteDuration = stats.NewLatencyHistogram15s32("idx.cassandra.delete")
	errmetrics         = cassandra.NewErrMetrics("idx.cassandra")

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

func ConfigSetup() *flag.FlagSet {
	casIdx := flag.NewFlagSet("cassandra-idx", flag.ExitOnError)

	casIdx.BoolVar(&Enabled, "enabled", true, "")
	casIdx.StringVar(&hosts, "hosts", "localhost:9042", "comma separated list of cassandra addresses in host:port form")
	casIdx.StringVar(&keyspace, "keyspace", "metrictank", "Cassandra keyspace to store metricDefinitions in.")
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
	return casIdx
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

// InitBare makes sure the keyspace, tables, and index exists in cassandra and creates a session
func (c *CasIdx) InitBare() error {
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
	tmpSession.Close()
	c.cluster.Keyspace = keyspace
	session, err := c.cluster.CreateSession()
	if err != nil {
		log.Error(3, "cassandra-idx failed to create cassandra session. %s", err)
		return err
	}

	c.session = session

	return nil
}

// Init makes sure the needed keyspace, table, index in cassandra exists, creates the session,
// rebuilds the in-memory index, sets up write queues, metrics and pruning routines
func (c *CasIdx) Init() error {
	log.Info("initializing cassandra-idx. Hosts=%s", hosts)
	if err := c.MemoryIdx.Init(); err != nil {
		return err
	}

	if err := c.InitBare(); err != nil {
		return err
	}

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

func (c *CasIdx) AddOrUpdate(data *schema.MetricData, partition int32) error {
	pre := time.Now()
	existing, inMemory := c.MemoryIdx.Get(data.Id)

	if inMemory {
		if existing.Partition == partition {
			oldest := time.Now().Add(-1 * updateInterval).Add(-1 * time.Duration(rand.Int63n(updateInterval.Nanoseconds()*int64(updateFuzzyness*100))))
			if existing.LastUpdate < oldest.Unix() {
				log.Debug("cassandra-idx def hasnt been seem for a while, updating index.")
				def := schema.MetricDefinitionFromMetricData(data)
				def.Partition = partition
				err := c.MemoryIdx.AddOrUpdateDef(def)
				if err != nil {
					c.writeQueue <- writeReq{recvTime: time.Now(), def: def}
				}
				statUpdateDuration.Value(time.Since(pre))
				return err
			}
			return nil
		}
		// the partition of the metric has changed. So we need to delete
		// the current metricDef from cassandra.  We do this is a separate
		// goroutine as we dont want to block waiting for the delete to succeed.
		go func() {
			if err := c.deleteDef(&existing); err != nil {
				log.Error(3, err.Error())
			}
		}()
		def := schema.MetricDefinitionFromMetricData(data)
		def.Partition = partition
		err := c.MemoryIdx.AddOrUpdateDef(def)
		if err == nil {
			c.writeQueue <- writeReq{recvTime: time.Now(), def: def}
		}
		statUpdateDuration.Value(time.Since(pre))
		return err
	}
	def := schema.MetricDefinitionFromMetricData(data)
	def.Partition = partition
	err := c.MemoryIdx.AddOrUpdateDef(def)
	if err == nil {
		c.writeQueue <- writeReq{recvTime: time.Now(), def: def}
	}
	statAddDuration.Value(time.Since(pre))
	return err
}

func (c *CasIdx) rebuildIndex() {
	log.Info("cassandra-idx Rebuilding Memory Index from metricDefinitions in Cassandra")
	pre := time.Now()
	var defs []schema.MetricDefinition
	for _, partition := range cluster.Manager.GetPartitions() {
		defs = c.LoadPartition(partition, defs)
	}
	num, err := c.MemoryIdx.Load(defs)
	if err != nil {
		log.Error(4, "cassandra-idx Rebuilding Memory Index partial failure. Imported %d out of %d. Took %s. First error: %s", num, len(defs), time.Since(pre), err)
	} else {
		log.Info("cassandra-idx Rebuilding Memory Index Complete. Imported %d. Took %s", num, time.Since(pre))
	}
}

func (c *CasIdx) Load(defs []schema.MetricDefinition) []schema.MetricDefinition {
	iter := c.session.Query("SELECT id, orgid, partition, name, metric, interval, unit, mtype, tags, lastupdate from metric_idx").Iter()
	return c.load(defs, iter)
}

func (c *CasIdx) LoadPartition(partition int32, defs []schema.MetricDefinition) []schema.MetricDefinition {
	iter := c.session.Query("SELECT id, orgid, partition, name, metric, interval, unit, mtype, tags, lastupdate from metric_idx where partition=?", partition).Iter()
	return c.load(defs, iter)
}

func (c *CasIdx) load(defs []schema.MetricDefinition, iter *gocql.Iter) []schema.MetricDefinition {
	mdef := schema.MetricDefinition{}
	var id, name, metric, unit, mtype string
	var orgId, interval int
	var partition int32
	var lastupdate int64
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
	return defs
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
		statQueryInsertWaitDuration.Value(time.Since(req.recvTime))
		pre := time.Now()
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

				statQueryInsertFail.Inc()
				errmetrics.Inc(err)
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
				statQueryInsertExecDuration.Value(time.Since(pre))
				statQueryInsertOk.Inc()
				log.Debug("cassandra-idx metricDef saved to cassandra. %s", req.def.Id)
			}
		}
	}
	log.Info("cassandra-idx writeQueue handler ended.")
	c.wg.Done()
}

func (c *CasIdx) Delete(orgId int, pattern string) ([]schema.MetricDefinition, error) {
	pre := time.Now()
	defs, err := c.MemoryIdx.Delete(orgId, pattern)
	if err != nil {
		return defs, err
	}
	for _, def := range defs {
		err = c.deleteDef(&def)
		if err != nil {
			log.Error(3, "cassandra-idx: %s", err.Error())
		}
	}
	statDeleteDuration.Value(time.Since(pre))
	return defs, err
}

func (c *CasIdx) deleteDef(def *schema.MetricDefinition) error {
	pre := time.Now()
	attempts := 0
	for attempts < 5 {
		attempts++
		err := c.session.Query("DELETE FROM metric_idx where partition=? AND id=?", def.Partition, def.Id).Exec()
		if err != nil {
			statQueryDeleteFail.Inc()
			errmetrics.Inc(err)
			log.Error(3, "cassandra-idx Failed to delete metricDef %s from cassandra. %s", def.Id, err)
			time.Sleep(time.Second)
		} else {
			statQueryDeleteOk.Inc()
			statQueryDeleteExecDuration.Value(time.Since(pre))
			return nil
		}
	}
	return fmt.Errorf("unable to delete metricDef %s from index after %d attempts.", def.Id, attempts)
}

func (c *CasIdx) Prune(orgId int, oldest time.Time) ([]schema.MetricDefinition, error) {
	pre := time.Now()
	pruned, err := c.MemoryIdx.Prune(orgId, oldest)
	// if an error was encountered then pruned is probably a partial list of metricDefs
	// deleted, so lets still try and delete these from Cassandra.
	for _, def := range pruned {
		log.Debug("cassandra-idx: metricDef %s pruned from the index.", def.Id)
		err := c.deleteDef(&def)
		if err != nil {
			log.Error(3, "cassandra-idx: %s", err.Error())
		}
	}
	statPruneDuration.Value(time.Since(pre))
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
