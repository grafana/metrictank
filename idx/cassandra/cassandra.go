package cassandra

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	"github.com/grafana/metrictank/cassandra"
	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/expr/tagquery"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/idx/memory"
	"github.com/grafana/metrictank/schema"
	"github.com/grafana/metrictank/stats"
	"github.com/grafana/metrictank/util"
	log "github.com/sirupsen/logrus"
	"golang.org/x/sync/errgroup"
)

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
	// metric idx.cassandra.save.skipped is how many saves have been skipped due to the writeQueue being full
	statSaveSkipped = stats.NewCounter32("idx.cassandra.save.skipped")
	errmetrics      = cassandra.NewErrMetrics("idx.cassandra")

	metaRecordRetryPolicy = gocql.ExponentialBackoffRetryPolicy{
		NumRetries: 10,
		Min:        time.Millisecond * time.Duration(10),
		Max:        time.Second,
	}
)

type writeReq struct {
	def      *schema.MetricDefinition
	recvTime time.Time
}

// CasIdx implements the the "MetricIndex" interface
type CasIdx struct {
	memory.MemoryIndex
	Config           *IdxConfig
	cluster          *gocql.ClusterConfig
	Session          *gocql.Session
	writeQueue       chan writeReq
	wg               sync.WaitGroup
	updateInterval32 uint32
}

type cqlIterator interface {
	Scan(dest ...interface{}) bool
	Close() error
}

func New(cfg *IdxConfig) *CasIdx {
	if err := cfg.Validate(); err != nil {
		log.Fatalf("cassandra-idx: %s", err)
	}
	cluster := gocql.NewCluster(strings.Split(cfg.Hosts, ",")...)
	cluster.Consistency = gocql.ParseConsistency(cfg.Consistency)
	cluster.Timeout = cfg.Timeout
	cluster.ConnectTimeout = cluster.Timeout
	cluster.NumConns = cfg.NumConns
	cluster.ProtoVersion = cfg.ProtoVer
	cluster.DisableInitialHostLookup = cfg.DisableInitialHostLookup
	if cfg.SSL {
		cluster.SslOpts = &gocql.SslOptions{
			CaPath:                 cfg.CaPath,
			EnableHostVerification: cfg.HostVerification,
		}
	}
	if cfg.Auth {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: cfg.Username,
			Password: cfg.Password,
		}
	}

	idx := &CasIdx{
		MemoryIndex:      memory.New(),
		Config:           cfg,
		cluster:          cluster,
		updateInterval32: uint32(cfg.updateInterval.Nanoseconds() / int64(time.Second)),
	}
	if cfg.updateCassIdx {
		idx.writeQueue = make(chan writeReq, cfg.writeQueueSize)
	}

	return idx
}

// InitBare makes sure the keyspace, tables, and index exists in cassandra and creates a session
func (c *CasIdx) InitBare() error {
	var err error
	tmpSession, err := c.cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("failed to create cassandra session: %s", err)
	}

	// read templates
	schemaKeyspace := util.ReadEntry(c.Config.SchemaFile, "schema_keyspace").(string)
	schemaTable := util.ReadEntry(c.Config.SchemaFile, "schema_table").(string)

	// create the keyspace or ensure it exists
	if c.Config.CreateKeyspace {
		log.Infof("cassandra-idx: ensuring that keyspace %s exists.", c.Config.Keyspace)
		err = tmpSession.Query(fmt.Sprintf(schemaKeyspace, c.Config.Keyspace)).Exec()
		if err != nil {
			return fmt.Errorf("failed to initialize cassandra keyspace: %s", err)
		}
		log.Infof("cassandra-idx: ensuring that table %s exists.", c.Config.Table)
		err = tmpSession.Query(fmt.Sprintf(schemaTable, c.Config.Keyspace, c.Config.Table)).Exec()
		if err != nil {
			return fmt.Errorf("failed to initialize cassandra table: %s", err)
		}
		c.EnsureTableExists(tmpSession, c.Config.SchemaFile, "schema_archive_table", c.Config.ArchiveTable)

		if memory.MetaTagSupport {
			c.EnsureTableExists(tmpSession, c.Config.SchemaFile, "schema_meta_record_table", c.Config.MetaRecordTable)
		}
	} else {
		var keyspaceMetadata *gocql.KeyspaceMetadata
		for attempt := 1; attempt > 0; attempt++ {
			keyspaceMetadata, err = tmpSession.KeyspaceMetadata(c.Config.Keyspace)
			if err != nil {
				if attempt >= 5 {
					return fmt.Errorf("cassandra keyspace %s not found. %d attempts", c.Config.Keyspace, attempt)
				}
				log.Warnf("cassandra-idx: cassandra keyspace %s not found. retrying in 5s. attempt: %d", c.Config.Keyspace, attempt)
				time.Sleep(5 * time.Second)
			} else {
				if _, ok := keyspaceMetadata.Tables[c.Config.Table]; ok {
					break
				} else {
					if attempt >= 5 {
						return fmt.Errorf("cassandra table %s not found. %d attempts", c.Config.Table, attempt)
					}
					log.Warnf("cassandra-idx: cassandra table %s not found. retrying in 5s. attempt: %d", c.Config.Table, attempt)
					time.Sleep(5 * time.Second)
				}
			}
		}
	}

	tmpSession.Close()
	c.cluster.Keyspace = c.Config.Keyspace
	session, err := c.cluster.CreateSession()
	if err != nil {
		return fmt.Errorf("failed to create cassandra session: %s", err)
	}

	c.Session = session

	return nil
}

// EnsureTableExists checks if the specified table exists or not. If it does not exist and the
// create-keyspace flag is true, then it will create it, if it doesn't exist and the create-keyspace
// flag is false, then it will return an error. If the table exists then it just returns nil
// sessions:    cassandra session
// schemaFile:  file containing table definition
// entryName:   identifier of the schema within the file
// tableName:   name of the table in cassandra
func (c *CasIdx) EnsureTableExists(session *gocql.Session, schemaFile, entryName, tableName string) error {
	var err error
	if session == nil {
		session, err = c.cluster.CreateSession()
		if err != nil {
			return fmt.Errorf("failed to create cassandra session: %s", err)
		}
	}

	tableSchema := util.ReadEntry(schemaFile, entryName).(string)

	if c.Config.CreateKeyspace {
		log.Infof("cassandra-idx: ensuring that table %s exists.", tableName)
		err = session.Query(fmt.Sprintf(tableSchema, c.Config.Keyspace, tableName)).Exec()
		if err != nil {
			return fmt.Errorf("failed to initialize cassandra table: %s", err)
		}
	} else {
		var keyspaceMetadata *gocql.KeyspaceMetadata
		keyspaceMetadata, err = session.KeyspaceMetadata(c.Config.Keyspace)
		if err != nil {
			return fmt.Errorf("failed to read cassandra tables: %s", err)
		}
		if _, ok := keyspaceMetadata.Tables[tableName]; !ok {
			return fmt.Errorf("table %s does not exist", tableName)
		}
	}
	return nil
}

// Init makes sure the needed keyspace, table, index in cassandra exists, creates the session,
// rebuilds the in-memory index, sets up write queues, metrics and pruning routines
func (c *CasIdx) Init() error {
	log.Infof("initializing cassandra-idx. Hosts=%s", c.Config.Hosts)
	if err := c.MemoryIndex.Init(); err != nil {
		return err
	}

	if err := c.InitBare(); err != nil {
		return err
	}

	if c.Config.updateCassIdx {
		c.wg.Add(c.Config.NumConns)
		for i := 0; i < c.Config.NumConns; i++ {
			go c.processWriteQueue()
		}
		log.Infof("cassandra-idx: started %d writeQueue handlers", c.Config.NumConns)
	}

	//Rebuild the in-memory index.
	c.rebuildIndex()

	if memory.IndexRules.Prunable() {
		go c.prune()
	}
	return nil
}

func (c *CasIdx) Stop() {
	log.Info("cassandra-idx: stopping")
	c.MemoryIndex.Stop()

	// if updateCassIdx is disabled then writeQueue should never have been initialized
	if c.Config.updateCassIdx {
		close(c.writeQueue)
	}
	c.wg.Wait()
	c.Session.Close()
}

// Update updates an existing archive, if found.
// It returns whether it was found, and - if so - the (updated) existing archive and its old partition
func (c *CasIdx) Update(point schema.MetricPoint, partition int32) (idx.Archive, int32, bool) {
	pre := time.Now()

	archive, oldPartition, inMemory := c.MemoryIndex.Update(point, partition)

	if !c.Config.updateCassIdx {
		statUpdateDuration.Value(time.Since(pre))
		return archive, oldPartition, inMemory
	}

	if inMemory {
		// Cassandra uses partition id as the partitioning key, so an "update" that changes the partition for
		// an existing metricDef will just create a new row in the table and won't remove the old row.
		// So we need to explicitly delete the old entry.
		if oldPartition != partition {
			c.deleteDefAsync(point.MKey, oldPartition)
		}

		// check if we need to save to cassandra.
		now := uint32(time.Now().Unix())
		if archive.LastSave < (now - c.updateInterval32) {
			archive = c.updateCassandra(now, inMemory, archive, partition)
		}
	}

	statUpdateDuration.Value(time.Since(pre))
	return archive, oldPartition, inMemory
}

func (c *CasIdx) AddOrUpdate(mkey schema.MKey, data *schema.MetricData, partition int32) (idx.Archive, int32, bool) {
	pre := time.Now()

	archive, oldPartition, inMemory := c.MemoryIndex.AddOrUpdate(mkey, data, partition)

	stat := statUpdateDuration
	if !inMemory {
		stat = statAddDuration
	}

	if !c.Config.updateCassIdx {
		stat.Value(time.Since(pre))
		return archive, oldPartition, inMemory
	}

	if inMemory {
		// Cassandra uses partition id as the partitioning key, so an "update" that changes the partition for
		// an existing metricDef will just create a new row in the table and won't remove the old row.
		// So we need to explicitly delete the old entry.
		if oldPartition != partition {
			c.deleteDefAsync(mkey, oldPartition)
		}
	}

	// check if we need to save to cassandra.
	now := uint32(time.Now().Unix())
	if archive.LastSave < (now - c.updateInterval32) {
		archive = c.updateCassandra(now, inMemory, archive, partition)
	}

	stat.Value(time.Since(pre))
	return archive, oldPartition, inMemory
}

// updateCassandra saves the archive to cassandra and
// updates the memory index with the updated fields.
func (c *CasIdx) updateCassandra(now uint32, inMemory bool, archive idx.Archive, partition int32) idx.Archive {
	// if the entry has not been saved for 1.5x updateInterval
	// then perform a blocking save.
	if archive.LastSave < (now - c.updateInterval32 - c.updateInterval32/2) {
		log.Debugf("cassandra-idx: updating def %s in index.", archive.MetricDefinition.Id)
		c.writeQueue <- writeReq{recvTime: time.Now(), def: &archive.MetricDefinition}
		archive.LastSave = now
		c.MemoryIndex.UpdateArchiveLastSave(archive.Id, archive.Partition, now)
	} else {
		// perform a non-blocking write to the writeQueue. If the queue is full, then
		// this will fail and we won't update the LastSave timestamp. The next time
		// the metric is seen, the previous lastSave timestamp will still be in place and so
		// we will try and save again.  This will continue until we are successful or the
		// lastSave timestamp become more then 1.5 x UpdateInterval, in which case we will
		// do a blocking write to the queue.
		select {
		case c.writeQueue <- writeReq{recvTime: time.Now(), def: &archive.MetricDefinition}:
			archive.LastSave = now
			c.MemoryIndex.UpdateArchiveLastSave(archive.Id, archive.Partition, now)
		default:
			statSaveSkipped.Inc()
			log.Debugf("cassandra-idx: writeQueue is full, update of %s not saved this time.", archive.MetricDefinition.Id)
		}
	}

	return archive
}

func (c *CasIdx) rebuildIndex() {
	log.Info("cassandra-idx: Rebuilding Memory Index from metricDefinitions in Cassandra")
	pre := time.Now()
	gate := make(chan struct{}, c.Config.InitLoadConcurrency)
	var wg sync.WaitGroup
	defPool := sync.Pool{
		New: func() interface{} {
			return []schema.MetricDefinition{}
		},
	}
	var num uint32
	for _, partition := range cluster.Manager.GetPartitions() {
		wg.Add(1)
		go func(p int32) {
			gate <- struct{}{}
			defs := defPool.Get().([]schema.MetricDefinition)
			defer func() {
				defPool.Put(defs[:0])
				wg.Done()
				<-gate
			}()
			defs = c.LoadPartitions([]int32{p}, defs, pre)
			atomic.AddUint32(&num, uint32(c.MemoryIndex.LoadPartition(p, defs)))
		}(partition)
	}

	if memory.MetaTagSupport {
		wg.Add(1)
		go func() {
			gate <- struct{}{}

			records := c.loadMetaRecords()
			for orgId, values := range records {
				c.applyMetaRecords(orgId, values)
			}

			wg.Done()
			<-gate
		}()
	}

	wg.Wait()
	log.Infof("cassandra-idx: Rebuilding Memory Index Complete. Imported %d. Took %s", num, time.Since(pre))
}

func (c *CasIdx) loadMetaRecords() map[uint32][]tagquery.MetaTagRecord {
	q := fmt.Sprintf("SELECT orgid, expressions, metatags FROM %s", c.Config.MetaRecordTable)
	var orgId uint32
	var expressions, metatags string
	iter := c.Session.Query(q).Iter()

	res := make(map[uint32][]tagquery.MetaTagRecord)
	for iter.Scan(&orgId, &expressions, &metatags) {
		record := tagquery.MetaTagRecord{}
		err := json.Unmarshal([]byte(expressions), &record.Expressions)
		if err != nil {
			log.Errorf("cassandra-idx: LoadMetaRecords() could not parse stored expressions (%s): %s", expressions, err)
			continue
		}

		err = json.Unmarshal([]byte(metatags), &record.MetaTags)
		if err != nil {
			log.Errorf("cassandra-idx: LoadMetaRecords() could not parse stored metatags (%s): %s", metatags, err)
			continue
		}

		res[orgId] = append(res[orgId], record)
	}

	return res
}

func (c *CasIdx) applyMetaRecords(orgId uint32, records []tagquery.MetaTagRecord) {
	for _, record := range records {
		_, _, err := c.MemoryIndex.MetaTagRecordUpsert(orgId, record, false)
		if err != nil {
			log.Errorf("cassandra-idx: applyMetaRecords() failed to apply meta record: %s", err)
		}
	}
}

func (c *CasIdx) MetaTagRecordUpsert(orgId uint32, upsertRecord tagquery.MetaTagRecord, persist bool) (tagquery.MetaTagRecord, bool, error) {
	record, created, err := c.MemoryIndex.MetaTagRecordUpsert(orgId, upsertRecord, persist)
	if err != nil {
		return record, created, err
	}

	if c.Config.updateCassIdx && persist {
		var err error

		// if a record has no meta tags associated with it, then we delete it
		if len(record.MetaTags) > 0 {
			err = c.persistMetaRecord(orgId, record, created)
		} else {
			err = c.deleteMetaRecord(orgId, record)
		}

		if err != nil {
			log.Errorf("Failed to update meta records in cassandra: %s", err)
			return record, created, fmt.Errorf("Failed to update cassandra: %s", err)
		}
	}

	return record, created, nil
}

func (c *CasIdx) MetaTagRecordSwap(orgId uint32, records []tagquery.MetaTagRecord, persist bool) (uint32, uint32, error) {
	added, deleted, err := c.MemoryIndex.MetaTagRecordSwap(orgId, records, persist)
	if !c.Config.updateCassIdx || err != nil {
		return added, deleted, err
	}

	now := time.Now().UnixNano() / 1000000
	batch := c.Session.NewBatch(gocql.LoggedBatch).RetryPolicy(&metaRecordRetryPolicy)

	// within a batch operation we need to specify timestamps using "USING TIMESTAMP" to ensure
	// that the statement execution happens in the order we require.
	// the leading DELETE statement gets the timestamp now - 1000 to ensure that it gets executed
	// before the sub-sequent inserts.
	batch.Query(fmt.Sprintf("DELETE FROM %s USING TIMESTAMP ? WHERE orgid=?", c.Config.MetaRecordTable), now-1000, orgId)
	var expressions, metaTags []byte
	var qry string

	for _, record := range records {
		record.Expressions.Sort()
		expressions, err = record.Expressions.MarshalJSON()
		if err != nil {
			return 0, 0, fmt.Errorf("Failed to marshal expressions: %s", err)
		}
		metaTags, err = record.MetaTags.MarshalJSON()
		if err != nil {
			return 0, 0, fmt.Errorf("Failed to marshal meta tags: %s", err)
		}
		qry = fmt.Sprintf("INSERT INTO %s (orgid, expressions, metatags, createdat, lastupdate) VALUES (?, ?, ?, ?, ?) USING TIMESTAMP ?", c.Config.MetaRecordTable)
		batch.Query(
			qry,
			orgId,
			expressions,
			metaTags,
			now,
			now,
			now)
	}

	err = c.Session.ExecuteBatch(batch)

	return added, deleted, err
}

func (c *CasIdx) persistMetaRecord(orgId uint32, record tagquery.MetaTagRecord, created bool) error {
	expressions, err := record.Expressions.MarshalJSON()
	if err != nil {
		return fmt.Errorf("Failed to marshal expressions: %s", err)
	}
	metaTags, err := record.MetaTags.MarshalJSON()
	if err != nil {
		return fmt.Errorf("Failed to marshal meta tags: %s", err)
	}

	now := time.Now().UnixNano() / 1000000

	if created {
		qry := fmt.Sprintf("INSERT INTO %s (orgid, expressions, metatags, createdat, lastupdate) VALUES (?, ?, ?, ?, ?)", c.Config.MetaRecordTable)
		return c.Session.Query(
			qry,
			orgId,
			expressions,
			metaTags,
			now,
			now).RetryPolicy(&metaRecordRetryPolicy).Exec()
	}

	qry := fmt.Sprintf("INSERT INTO %s (orgid, expressions, metatags, lastupdate) VALUES (?, ?, ?, ?)", c.Config.MetaRecordTable)
	return c.Session.Query(
		qry,
		orgId,
		expressions,
		metaTags,
		now).RetryPolicy(&metaRecordRetryPolicy).Exec()
}

func (c *CasIdx) deleteMetaRecord(orgId uint32, record tagquery.MetaTagRecord) error {
	expressions, err := record.Expressions.MarshalJSON()
	if err != nil {
		return fmt.Errorf("Failed to marshal record expressions: %s", err)
	}

	qry := fmt.Sprintf("DELETE FROM %s WHERE orgid=? AND expressions=?", c.Config.MetaRecordTable)
	return c.Session.Query(
		qry,
		orgId,
		expressions,
	).RetryPolicy(&metaRecordRetryPolicy).Exec()
}

func (c *CasIdx) Load(defs []schema.MetricDefinition, now time.Time) []schema.MetricDefinition {
	iter := c.Session.Query(fmt.Sprintf("SELECT id, orgid, partition, name, interval, unit, mtype, tags, lastupdate from %s", c.Config.Table)).Iter()
	return c.load(defs, iter, now)
}

// LoadPartitions appends MetricDefinitions from the given partitions to defs and returns the modified defs, honoring pruning settings relative to now
func (c *CasIdx) LoadPartitions(partitions []int32, defs []schema.MetricDefinition, now time.Time) []schema.MetricDefinition {
	placeholders := make([]string, len(partitions))
	for i, p := range partitions {
		placeholders[i] = strconv.Itoa(int(p))
	}
	q := fmt.Sprintf("SELECT id, orgid, partition, name, interval, unit, mtype, tags, lastupdate from %s where partition in (%s)", c.Config.Table, strings.Join(placeholders, ","))
	iter := c.Session.Query(q).Iter()
	return c.load(defs, iter, now)
}

// load appends MetricDefinitions from the iterator to defs and returns the modified defs, honoring pruning settings relative to now
func (c *CasIdx) load(defs []schema.MetricDefinition, iter cqlIterator, now time.Time) []schema.MetricDefinition {
	defsByNames := make(map[string][]*schema.MetricDefinition)
	var id, name, unit, mtype string
	var orgId, interval int
	var partition int32
	var lastupdate int64
	var tags []string
	for iter.Scan(&id, &orgId, &partition, &name, &interval, &unit, &mtype, &tags, &lastupdate) {
		mkey, err := schema.MKeyFromString(id)
		if err != nil {
			log.Errorf("cassandra-idx: load() could not parse ID %q: %s -> skipping", id, err)
			continue
		}
		if orgId < 0 {
			orgId = int(idx.OrgIdPublic)
		}

		mdef := &schema.MetricDefinition{
			Id:         mkey,
			OrgId:      uint32(orgId),
			Partition:  partition,
			Name:       name,
			Interval:   interval,
			Unit:       unit,
			Mtype:      mtype,
			Tags:       tags,
			LastUpdate: lastupdate,
		}
		nameWithTags := mdef.NameWithTags()
		defsByNames[nameWithTags] = append(defsByNames[nameWithTags], mdef)
	}
	if err := iter.Close(); err != nil {
		log.Fatalf("Could not close iterator: %s", err.Error())
	}

	// getting all cutoffs once saves having to recompute everytime we have a match
	cutoffs := memory.IndexRules.Cutoffs(now)

NAMES:
	for nameWithTags, defsByName := range defsByNames {
		irId, _ := memory.IndexRules.Match(nameWithTags)
		cutoff := cutoffs[irId]
		for _, def := range defsByName {
			if def.LastUpdate >= cutoff {
				// if any of the defs for a given nameWithTags is not stale, then we need to load
				// all the defs for that nameWithTags.
				for _, defToAdd := range defsByNames[nameWithTags] {
					defs = append(defs, *defToAdd)
				}
				continue NAMES
			}
		}
	}

	return defs
}

// ArchiveDefs writes each of the provided defs to the archive table and
// then deletes the defs from the metric index table.
func (c *CasIdx) ArchiveDefs(defs []schema.MetricDefinition) (int, error) {
	defChan := make(chan *schema.MetricDefinition, c.Config.NumConns)
	g, ctx := errgroup.WithContext(context.Background())

	// keep track of how many defs were successfully archived.
	success := make([]int, c.Config.NumConns)

	for i := 0; i < c.Config.NumConns; i++ {
		i := i
		g.Go(func() error {
			for {
				select {
				case def, ok := <-defChan:
					if !ok {
						return nil
					}
					err := c.addDefToArchive(*def)
					if err != nil {
						// If we failed to add the def to the archive table then just continue on to the next def.
						// As we haven't yet removed the this def from the metric index table yet, the next time archiving
						// is performed the this def will be processed again. As no action is needed by an operator, we
						// just log this as a warning.
						log.Warnf("cassandra-idx: Failed add def to archive table. error=%s. def=%+v", err, *def)
						continue
					}

					err = c.deleteDef(def.Id, def.Partition)
					if err != nil {
						// The next time archiving is performed this def will be processed again. Re-adding the def to the archive
						// table will just be treated like an update with only the archived_at field changing. As no action is needed
						// by an operator, we just log this as a warning.
						log.Warnf("cassandra-idx: Failed to remove archived def from %s table. error=%s. def=%+v", c.Config.Table, err, *def)
						continue
					}

					// increment counter of defs successfully archived
					success[i] = success[i] + 1
				case <-ctx.Done():
					return ctx.Err()
				}
			}
		})
	}
	for i := range defs {
		defChan <- &defs[i]
	}
	close(defChan)

	// wait for all goroutines to complete.
	err := g.Wait()

	// get the count of defs successfully archived.
	total := 0
	for _, count := range success {
		total = total + count
	}

	return total, err
}

func (c *CasIdx) processWriteQueue() {
	var success bool
	var attempts int
	var err error
	var req writeReq
	qry := fmt.Sprintf("INSERT INTO %s (id, orgid, partition, name, interval, unit, mtype, tags, lastupdate) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)", c.Config.Table)
	for req = range c.writeQueue {
		if err != nil {
			log.Errorf("Failed to marshal metricDef: %s. value was: %+v", err, *req.def)
			continue
		}
		statQueryInsertWaitDuration.Value(time.Since(req.recvTime))
		pre := time.Now()
		success = false
		attempts = 0

		for !success {
			if err := c.Session.Query(
				qry,
				req.def.Id.String(),
				req.def.OrgId,
				req.def.Partition,
				req.def.Name,
				req.def.Interval,
				req.def.Unit,
				req.def.Mtype,
				req.def.Tags,
				req.def.LastUpdate).Exec(); err != nil {

				statQueryInsertFail.Inc()
				errmetrics.Inc(err)
				if (attempts % 20) == 0 {
					log.Warnf("cassandra-idx: Failed to write def to cassandra. it will be retried. %s. the value was: %+v", err, *req.def)
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
				log.Debugf("cassandra-idx: metricDef %s saved to cassandra", req.def.Id)
			}
		}
	}
	log.Info("cassandra-idx: writeQueue handler ended.")
	c.wg.Done()
}

func (c *CasIdx) addDefToArchive(def schema.MetricDefinition) error {
	insertQry := fmt.Sprintf("INSERT INTO %s (id, orgid, partition, name, interval, unit, mtype, tags, lastupdate, archived_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)", c.Config.ArchiveTable)
	maxAttempts := 5
	now := time.Now().UTC().Unix()
	var err error

	for attempts := 0; attempts < maxAttempts; attempts++ {
		if attempts > 0 {
			sleepTime := 100 * attempts
			if sleepTime > 2000 {
				sleepTime = 2000
			}
			time.Sleep(time.Duration(sleepTime) * time.Millisecond)
		}

		err := c.Session.Query(
			insertQry,
			def.Id.String(),
			def.OrgId,
			def.Partition,
			def.Name,
			def.Interval,
			def.Unit,
			def.Mtype,
			def.Tags,
			def.LastUpdate,
			now).Exec()

		if err == nil {
			return nil
		}

		// log first failure as a warning.  If we reach max attempts, the error will bubble up to the caller.
		if attempts == 0 {
			log.Warnf("cassandra-idx: Failed to write def to cassandra. it will be retried. error=%s. def=%+v", err, def)
		}
	}

	return err
}

func (c *CasIdx) Delete(orgId uint32, pattern string) ([]idx.Archive, error) {
	pre := time.Now()
	defs, err := c.MemoryIndex.Delete(orgId, pattern)
	if err != nil {
		return defs, err
	}
	if c.Config.updateCassIdx {
		for _, def := range defs {
			err = c.deleteDef(def.Id, def.Partition)
			if err != nil {
				log.Errorf("cassandra-idx: %s", err.Error())
			}
		}
	}
	statDeleteDuration.Value(time.Since(pre))
	return defs, err
}

func (c *CasIdx) deleteDef(key schema.MKey, part int32) error {
	pre := time.Now()
	attempts := 0
	keyStr := key.String()
	for attempts < 5 {
		attempts++
		err := c.Session.Query(fmt.Sprintf("DELETE FROM %s where partition=? AND id=?", c.Config.Table), part, keyStr).Exec()
		if err != nil {
			statQueryDeleteFail.Inc()
			errmetrics.Inc(err)
			log.Warnf("cassandra-idx: Failed to delete metricDef %s from cassandra: %s", keyStr, err)
			time.Sleep(time.Second)
		} else {
			statQueryDeleteOk.Inc()
			statQueryDeleteExecDuration.Value(time.Since(pre))
			return nil
		}
	}
	return fmt.Errorf("unable to delete metricDef %s from index after %d attempts.", keyStr, attempts)
}

func (c *CasIdx) deleteDefAsync(key schema.MKey, part int32) {
	go func() {
		if err := c.deleteDef(key, part); err != nil {
			log.Errorf("cassandra-idx: %s", err.Error())
		}
	}()
}

func (c *CasIdx) Prune(now time.Time) ([]idx.Archive, error) {
	log.Info("cassandra-idx: start pruning of series")
	pruned, err := c.MemoryIndex.Prune(now)
	duration := time.Since(now)
	if err != nil {
		log.Errorf("cassandra-idx: pruning error: %s", err)
	} else {
		statPruneDuration.Value(duration)
		log.Infof("cassandra-idx: finished pruning of %d series in %s", len(pruned), duration)
	}
	return pruned, err
}

func (c *CasIdx) prune() {
	ticker := time.NewTicker(c.Config.pruneInterval)
	for now := range ticker.C {
		c.Prune(now)
	}
}
