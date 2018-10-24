package bigtable

import (
	"context"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/idx/memory"
	"github.com/grafana/metrictank/stats"
	"github.com/raintank/schema"
	log "github.com/sirupsen/logrus"
)

const COLUMN_FAMILY = "idx"

var (
	// metric idx.bigtable.query-insert.ok is how many insert queries for a metric completed successfully (triggered by an add or an update)
	statQueryInsertOk = stats.NewCounter32("idx.bigtable.query-insert.ok")
	// metric idx.bigtable.query-insert.fail is how many insert queries for a metric failed (triggered by an add or an update)
	statQueryInsertFail = stats.NewCounter32("idx.bigtable.query-insert.fail")
	// metric idx.bigtable.query-delete.ok is how many delete queries for a metric completed successfully (triggered by an update or a delete)
	statQueryDeleteOk = stats.NewCounter32("idx.bigtable.query-delete.ok")
	// metric idx.bigtable.query-delete.fail is how many delete queries for a metric failed (triggered by an update or a delete)
	statQueryDeleteFail = stats.NewCounter32("idx.bigtable.query-delete.fail")

	// metric idx.bigtable.query-insert.wait is time inserts spent in queue before being executed
	statQueryInsertWaitDuration = stats.NewLatencyHistogram12h32("idx.bigtable.query-insert.wait")
	// metric idx.bigtable.query-insert.exec is time spent executing inserts (possibly repeatedly until success)
	statQueryInsertExecDuration = stats.NewLatencyHistogram15s32("idx.bigtable.query-insert.exec")
	// metric idx.bigtable.query-delete.exec is time spent executing deletes (possibly repeatedly until success)
	statQueryDeleteExecDuration = stats.NewLatencyHistogram15s32("idx.bigtable.query-delete.exec")

	// metric idx.bigtable.add is the duration of an add of one metric to the bigtable idx, including the add to the in-memory index, excluding the insert query
	statAddDuration = stats.NewLatencyHistogram15s32("idx.bigtable.add")
	// metric idx.bigtable.update is the duration of an update of one metric to the bigtable idx, including the update to the in-memory index, excluding any insert/delete queries
	statUpdateDuration = stats.NewLatencyHistogram15s32("idx.bigtable.update")
	// metric idx.bigtable.prune is the duration of a prune of the bigtable idx, including the prune of the in-memory index and all needed delete queries
	statPruneDuration = stats.NewLatencyHistogram15s32("idx.bigtable.prune")
	// metric idx.bigtable.delete is the duration of a delete of one or more metrics from the bigtable idx, including the delete from the in-memory index and the delete query
	statDeleteDuration = stats.NewLatencyHistogram15s32("idx.bigtable.delete")
	// metric idx.bigtable.save.skipped is how many saves have been skipped due to the writeQueue being full
	statSaveSkipped = stats.NewCounter32("idx.bigtable.save.skipped")
	// metric idx.bigtable.save.bytes-per-request is the number of bytes written to bigtable in each request.
	statSaveBytesPerRequest = stats.NewMeter32("idx.bigtable.save.bytes-per-request", true)
)

type writeReq struct {
	def      *schema.MetricDefinition
	recvTime time.Time
}

type BigtableIdx struct {
	memory.MemoryIdx
	cfg        *IdxConfig
	tbl        *bigtable.Table
	client     *bigtable.Client
	writeQueue chan writeReq
	shutdown   chan struct{}
	wg         sync.WaitGroup
}

func New(cfg *IdxConfig) *BigtableIdx {
	// Hopefully the caller has already validated their config, but just in case,
	// lets make sure.
	if err := cfg.Validate(); err != nil {
		log.Fatalf("bigtable-idx: %s", err)
	}
	idx := &BigtableIdx{
		MemoryIdx: *memory.New(),
		cfg:       cfg,
		shutdown:  make(chan struct{}),
	}
	if cfg.UpdateBigtableIdx {
		idx.writeQueue = make(chan writeReq, cfg.WriteQueueSize-cfg.WriteMaxFlushSize)
	}
	return idx
}

// InitBare creates the client and makes sure the tables and columFamilies exist.
// It also opens the table for reads/writes.
func (b *BigtableIdx) InitBare() error {
	ctx := context.Background()
	if b.cfg.CreateCF {
		adminClient, err := bigtable.NewAdminClient(ctx, b.cfg.GcpProject, b.cfg.BigtableInstance)
		if err != nil {
			log.Errorf("bigtable-idx: failed to create bigtable admin client. %s", err)
			return err
		}
		tables, err := adminClient.Tables(ctx)
		if err != nil {
			log.Errorf("bigtable-idx: failed to list tables. %s", err)
			return err
		}
		found := false
		for _, t := range tables {
			if t == b.cfg.TableName {
				found = true
				break
			}
		}
		if !found {
			log.Infof("bigtable-idx: table %s does not yet exist. Creating it.", b.cfg.TableName)
			table := bigtable.TableConf{
				TableID: b.cfg.TableName,
				Families: map[string]bigtable.GCPolicy{
					COLUMN_FAMILY: bigtable.MaxVersionsPolicy(1),
				},
			}
			err := adminClient.CreateTableFromConf(ctx, &table)
			if err != nil {
				log.Errorf("bigtable-idx: failed to create %s table. %s", b.cfg.TableName, err)
				return err
			}
		} else {
			log.Infof("bigtable-idx: table %s exists.", b.cfg.TableName)
			// table exists.  Lets make sure that it has all of the CF's we need.
			table, err := adminClient.TableInfo(ctx, b.cfg.TableName)
			if err != nil {
				log.Errorf("bigtable-idx: failed to get tableInfo of %s. %s", b.cfg.TableName, err)
				return err
			}
			existingFamilies := make(map[string]string)
			for _, cf := range table.FamilyInfos {
				existingFamilies[cf.Name] = cf.GCPolicy
			}
			policy, ok := existingFamilies[COLUMN_FAMILY]
			if !ok {
				log.Infof("bigtable-idx: column family %s/%s does not exist. Creating it.", b.cfg.TableName, COLUMN_FAMILY)
				err = adminClient.CreateColumnFamily(ctx, b.cfg.TableName, COLUMN_FAMILY)
				if err != nil {
					log.Errorf("bigtable-idx: failed to create cf %s/%s. %s", b.cfg.TableName, COLUMN_FAMILY, err)
					return err
				}
				err = adminClient.SetGCPolicy(ctx, b.cfg.TableName, COLUMN_FAMILY, bigtable.MaxVersionsPolicy(1))
				if err != nil {
					log.Errorf("bigtable-idx: failed to set GCPolicy of %s/%s. %s", b.cfg.TableName, COLUMN_FAMILY, err)
					return err
				}
			} else if policy == "" {
				log.Infof("bigtable-idx: column family %s/%s exists but has no GCPolicy. Creating it.", b.cfg.TableName, COLUMN_FAMILY)
				err = adminClient.SetGCPolicy(ctx, b.cfg.TableName, COLUMN_FAMILY, bigtable.MaxVersionsPolicy(1))
				if err != nil {
					log.Errorf("bigtable-idx: failed to set GCPolicy of %s/%s. %s", b.cfg.TableName, COLUMN_FAMILY, err)
					return err
				}
			}
		}
	}
	client, err := bigtable.NewClient(ctx, b.cfg.GcpProject, b.cfg.BigtableInstance)
	if err != nil {
		log.Errorf("bigtable-idx: failed to create bigtable client. %s", err)
		return err
	}

	b.client = client
	b.tbl = client.Open(b.cfg.TableName)
	return nil
}

// Init makes sure the tables and columFamilies exist. It also opens the table for reads/writes. Then
// rebuilds the in-memory index, sets up write queues, metrics and pruning routines
func (b *BigtableIdx) Init() error {
	log.Infof("bigtable-idx: Initializing. Project=%s, Instance=%s", b.cfg.GcpProject, b.cfg.BigtableInstance)
	if err := b.MemoryIdx.Init(); err != nil {
		return err
	}

	if err := b.InitBare(); err != nil {
		return err
	}

	if b.cfg.UpdateBigtableIdx {
		b.wg.Add(b.cfg.WriteConcurrency)
		for i := 0; i < b.cfg.WriteConcurrency; i++ {
			go b.processWriteQueue()
		}
		log.Infof("bigtable-idx: started %d writeQueue handlers", b.cfg.WriteConcurrency)
	}

	b.rebuildIndex()
	if memory.IndexRules.Prunable() {
		b.wg.Add(1)
		go b.prune()
	}
	return nil
}

func (b *BigtableIdx) Stop() {
	b.MemoryIdx.Stop()
	close(b.shutdown)
	if b.cfg.UpdateBigtableIdx {
		close(b.writeQueue)
	}
	b.wg.Wait()

	err := b.client.Close()
	if err != nil {
		log.Errorf("bigtable-idx: Error closing bigtable client. %s", err)
	}
}

// Update updates an existing archive, if found.
// It returns whether it was found, and - if so - the (updated) existing archive and its old partition
func (b *BigtableIdx) Update(point schema.MetricPoint, partition int32) (idx.Archive, int32, bool) {
	pre := time.Now()

	archive, oldPartition, inMemory := b.MemoryIdx.Update(point, partition)

	if !b.cfg.UpdateBigtableIdx {
		statUpdateDuration.Value(time.Since(pre))
		return archive, oldPartition, inMemory
	}

	if inMemory {
		// bigtable uses partition ID in the key prefix, so an "update" that changes the partition for
		// an existing metricDef will just create a new row in the table and won't remove the old row.
		// So we need to explicitly delete the old entry.
		if oldPartition != partition {
			go func() {
				err := b.deleteRow(FormatRowKey(archive.Id, oldPartition))
				if err != nil {
					log.Errorf("bigtable-idx: failed to delete row. %s", err)
				}
			}()
		}
		// check if we need to save to bigtable.
		now := uint32(time.Now().Unix())
		if archive.LastSave < (now - b.cfg.updateInterval32) {
			archive = b.updateBigtable(now, inMemory, archive, partition)
		}
	}

	statUpdateDuration.Value(time.Since(pre))
	return archive, oldPartition, inMemory
}

func (b *BigtableIdx) AddOrUpdate(mkey schema.MKey, data *schema.MetricData, partition int32) (idx.Archive, int32, bool) {
	pre := time.Now()

	archive, oldPartition, inMemory := b.MemoryIdx.AddOrUpdate(mkey, data, partition)

	stat := statUpdateDuration
	if !inMemory {
		stat = statAddDuration
	}

	if !b.cfg.UpdateBigtableIdx {
		stat.Value(time.Since(pre))
		return archive, oldPartition, inMemory
	}

	if inMemory {
		// bigtable uses partition ID in the key prefix, so an "update" that changes the partition for
		// an existing metricDef will just create a new row in the table and won't remove the old row.
		// So we need to explicitly delete the old entry.
		if oldPartition != partition {
			go func() {
				err := b.deleteRow(FormatRowKey(archive.Id, oldPartition))
				if err != nil {
					log.Errorf("bigtable-idx: failed to delete row. %s", err)
				}
			}()
		}
	}

	// check if we need to save to bigtable.
	now := uint32(time.Now().Unix())
	if archive.LastSave < (now - b.cfg.updateInterval32) {
		archive = b.updateBigtable(now, inMemory, archive, partition)
	}

	stat.Value(time.Since(pre))
	return archive, oldPartition, inMemory
}

// updateBigtable saves the archive to bigtable and
// updates the memory index with the updated fields.
func (b *BigtableIdx) updateBigtable(now uint32, inMemory bool, archive idx.Archive, partition int32) idx.Archive {
	// if the entry has not been saved for 1.5x updateInterval
	// then perform a blocking save.
	if archive.LastSave < (now - b.cfg.updateInterval32 - (b.cfg.updateInterval32 / 2)) {
		log.Debugf("bigtable-idx: updating def %s in index.", archive.MetricDefinition.Id)
		b.writeQueue <- writeReq{recvTime: time.Now(), def: &archive.MetricDefinition}
		archive.LastSave = now
		b.MemoryIdx.UpdateArchive(archive)
	} else {
		// perform a non-blocking write to the writeQueue. If the queue is full, then
		// this will fail and we won't update the LastSave timestamp. The next time
		// the metric is seen, the previous lastSave timestamp will still be in place and so
		// we will try and save again.  This will continue until we are successful or the
		// lastSave timestamp become more then 1.5 x UpdateInterval, in which case we will
		// do a blocking write to the queue.
		select {
		case b.writeQueue <- writeReq{recvTime: time.Now(), def: &archive.MetricDefinition}:
			archive.LastSave = now
			b.MemoryIdx.UpdateArchive(archive)
		default:
			statSaveSkipped.Inc()
			log.Debugf("bigtable-idx: writeQueue is full, update of %s not saved this time", archive.MetricDefinition.Id)
		}
	}

	return archive
}

func (b *BigtableIdx) rebuildIndex() {
	log.Info("bigtable-idx: Rebuilding Memory Index from metricDefinitions in bigtable")
	pre := time.Now()

	num := 0
	var defs []schema.MetricDefinition
	for _, partition := range cluster.Manager.GetPartitions() {
		defs = b.LoadPartition(partition, defs[:0], pre)
		num += b.MemoryIdx.Load(defs)
	}

	log.Infof("bigtable-idx: Rebuilding Memory Index Complete. Imported %d. Took %s", num, time.Since(pre))
}

func (b *BigtableIdx) LoadPartition(partition int32, defs []schema.MetricDefinition, now time.Time) []schema.MetricDefinition {
	ctx := context.Background()
	rr := bigtable.PrefixRange(fmt.Sprintf("%d_", partition))
	defsByNames := make(map[string][]schema.MetricDefinition)
	var marshalErr error
	err := b.tbl.ReadRows(ctx, rr, func(r bigtable.Row) bool {
		def := schema.MetricDefinition{}
		marshalErr = RowToSchema(r, &def)
		if marshalErr != nil {
			return false
		}
		log.Debugf("bigtable-idx: found def %+v", def)
		nameWithTags := def.NameWithTags()
		defsByNames[nameWithTags] = append(defsByNames[nameWithTags], def)
		return true
	}, bigtable.RowFilter(bigtable.FamilyFilter(COLUMN_FAMILY)))
	if err != nil {
		log.Fatalf("bigtable-idx: failed to load defs from Bigtable. %s", err)
	}
	if marshalErr != nil {
		log.Fatalf("bigtable-idx: failed to marshal row to metricDef. %s", marshalErr)
	}

	// getting all cutoffs once saves having to recompute everytime we have a match
	cutoffs := memory.IndexRules.Cutoffs(now)

NAMES:
	for nameWithTags, defsByName := range defsByNames {
		irId, _ := memory.IndexRules.Match(nameWithTags)
		cutoff := cutoffs[irId]
		for _, def := range defsByName {
			if def.LastUpdate > cutoff {
				// if any of the defs for a given nameWithTags is not stale, then we need to load
				// all the defs for that nameWithTags.
				defs = append(defs, defsByNames[nameWithTags]...)
				continue NAMES
			}
		}
		// all defs are stale
		delete(defsByNames, nameWithTags)
	}

	return defs
}

func (b *BigtableIdx) processWriteQueue() {
	timer := time.NewTimer(time.Second)
	buffer := make([]writeReq, 0)

	flush := func() {
		rowKeys := make([]string, len(buffer))
		mutations := make([]*bigtable.Mutation, len(buffer))
		byteCount := 0
		for i, req := range buffer {
			statQueryInsertWaitDuration.Value(time.Since(req.recvTime))
			key, cols := SchemaToRow(req.def)
			rowKeys[i] = key
			mut := bigtable.NewMutation()
			for col, val := range cols {
				mut.Set(COLUMN_FAMILY, col, bigtable.Now(), val)
				byteCount += len(val)
			}
			mutations[i] = mut
		}
		statSaveBytesPerRequest.Value(byteCount)
		pre := time.Now()
		complete := false
		attempts := 0
		for !complete {
			errs, err := b.tbl.ApplyBulk(context.Background(), rowKeys, mutations)
			if err != nil {
				statQueryInsertFail.Add(len(rowKeys))
				log.Errorf("bigtable-idx: Failed to write %d defs to bigtable. they won't be retried. %s", len(rowKeys), err)
				complete = true
			} else if len(errs) > 0 {
				var failedRowKeys []string
				var failedMutations []*bigtable.Mutation
				for i, err := range errs {
					if err != nil {
						failedRowKeys = append(failedRowKeys, rowKeys[i])
						failedMutations = append(failedMutations, mutations[i])
					}
				}
				log.Warnf("bigtable-idx: failed to write %d/%d rows.  They will be retried. %s", len(failedRowKeys), len(rowKeys), err)
				statQueryInsertFail.Add(len(failedRowKeys))

				rowKeys = failedRowKeys
				mutations = failedMutations

				sleepTime := 100 * attempts
				if sleepTime > 2000 {
					sleepTime = 2000
				}
				time.Sleep(time.Duration(sleepTime) * time.Millisecond)
				attempts++
			} else {
				complete = true
				statQueryInsertExecDuration.Value(time.Since(pre))
				statQueryInsertOk.Add(len(rowKeys))
				log.Debugf("bigtable-idx: %d metricDefs saved to bigtable.", len(rowKeys))
			}
		}
		buffer = buffer[:0]
	}

LOOP:
	for {
		select {
		case <-timer.C:
			timer.Reset(time.Second)
			if len(buffer) > 0 {
				flush()
			}
		case req, ok := <-b.writeQueue:
			if !ok {
				// writeQueue was closed.  Flush and exit.
				timer.Stop()
				flush()
				break LOOP
			}
			buffer = append(buffer, req)
			if len(buffer) >= b.cfg.WriteMaxFlushSize {
				// make sure the timer hasn't already fired. If it has we read
				// from the chan and consume the event.
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(time.Second)
				flush()
			}
		}
	}
	log.Info("bigtable-idx: writeQueue handler ended.")
	b.wg.Done()
}

func (b *BigtableIdx) Delete(orgId uint32, pattern string) ([]idx.Archive, error) {
	pre := time.Now()
	defs, err := b.MemoryIdx.Delete(orgId, pattern)
	if err != nil {
		return defs, err
	}
	if b.cfg.UpdateBigtableIdx {
		for _, def := range defs {
			delErr := b.deleteDef(&def.MetricDefinition)
			// the last error encountered will be passed back to the caller
			if delErr != nil {
				err = delErr
			}
		}
	}
	statDeleteDuration.Value(time.Since(pre))
	return defs, err
}

func (b *BigtableIdx) deleteDef(def *schema.MetricDefinition) error {
	return b.deleteRow(FormatRowKey(def.Id, def.Partition))
}

func (b *BigtableIdx) deleteRow(key string) error {
	pre := time.Now()
	mut := bigtable.NewMutation()
	mut.DeleteRow()
	err := b.tbl.Apply(context.Background(), key, mut)
	if err != nil {
		statQueryDeleteFail.Inc()
		log.Errorf("bigtable-idx: Failed to delete row %s from bigtable. %s", key, err)
		return err
	}

	statQueryDeleteOk.Inc()
	statQueryDeleteExecDuration.Value(time.Since(pre))
	return nil
}

func (b *BigtableIdx) Prune(now time.Time) ([]idx.Archive, error) {
	log.Info("bigtable-idx: start pruning of series")
	pruned, err := b.MemoryIdx.Prune(now)
	duration := time.Since(now)
	if err != nil {
		log.Errorf("bigtable-idx: prune error. %s", err)
	} else {
		statPruneDuration.Value(duration)
		log.Infof("bigtable-idx: finished pruning of %d series in %s", len(pruned), duration)

	}
	return pruned, err
}

func (b *BigtableIdx) prune() {
	defer b.wg.Done()
	ticker := time.NewTicker(b.cfg.PruneInterval)
	for {
		select {
		case now := <-ticker.C:
			b.Prune(now)
		case <-b.shutdown:
			return
		}
	}
}
