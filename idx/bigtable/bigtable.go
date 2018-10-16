package bigtable

import (
	"context"
	"flag"
	"fmt"
	"sync"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/idx/memory"
	"github.com/grafana/metrictank/stats"
	"github.com/raintank/schema"
	"github.com/rakyll/globalconf"
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

	Enabled           bool
	maxStale          time.Duration
	pruneInterval     time.Duration
	updateBigTableIdx bool
	updateInterval    time.Duration
	updateInterval32  uint32

	writeQueueSize    int
	writeConcurrency  int
	writeMaxFlushSize int

	bigtableInstance string
	gcpProject       string
	tableName        string
	createCF         bool
)

func ConfigSetup() {
	btIdx := flag.NewFlagSet("bigtable-idx", flag.ExitOnError)

	btIdx.BoolVar(&Enabled, "enabled", false, "")
	btIdx.StringVar(&gcpProject, "gcp-project", "default", "Name of GCP project the bigtable cluster resides in")
	btIdx.StringVar(&bigtableInstance, "bigtable-instance", "default", "Name of bigtable instance")
	btIdx.StringVar(&tableName, "table-name", "metric_idx", "Name of bigtable table used for metricDefs")
	btIdx.IntVar(&writeQueueSize, "write-queue-size", 100000, "Max number of metricDefs allowed to be unwritten to bigtable. Must be larger then write-max-flush-size")
	btIdx.IntVar(&writeMaxFlushSize, "write-max-flush-size", 10000, "Max number of metricDefs in each batch write to bigtable")
	btIdx.IntVar(&writeConcurrency, "write-concurrency", 5, "Number of writer threads to use")
	btIdx.BoolVar(&updateBigTableIdx, "update-bigtable-index", true, "synchronize index changes to bigtable. not all your nodes need to do this.")
	btIdx.DurationVar(&updateInterval, "update-interval", time.Hour*3, "frequency at which we should update the metricDef lastUpdate field, use 0s for instant updates")
	btIdx.DurationVar(&maxStale, "max-stale", 0, "clear series from the index if they have not been seen for this much time.")
	btIdx.DurationVar(&pruneInterval, "prune-interval", time.Hour*3, "Interval at which the index should be checked for stale series.")
	btIdx.BoolVar(&createCF, "create-cf", true, "enable the creation of the table and column families")

	globalconf.Register("bigtable-idx", btIdx)
	return
}

func ConfigProcess() {
	return
}

type writeReq struct {
	def      *schema.MetricDefinition
	recvTime time.Time
}

type BigtableIdx struct {
	memory.MemoryIdx
	tbl        *bigtable.Table
	client     *bigtable.Client
	writeQueue chan writeReq
	shutdown   chan struct{}
	wg         sync.WaitGroup
}

func New() *BigtableIdx {
	if writeMaxFlushSize >= writeQueueSize {
		log.Fatal("bigtable-idx: write-queue-size must be larger then write-max-flush-size")
	}
	ctx := context.Background()
	if createCF {
		adminClient, err := bigtable.NewAdminClient(ctx, gcpProject, bigtableInstance)
		if err != nil {
			log.Fatalf("bigtable-idx: failed to create bigtable admin client. %s", err)
		}
		tables, err := adminClient.Tables(ctx)
		if err != nil {
			log.Fatalf("bigtable-idx: failed to list tables. %s", err)
		}
		found := false
		for _, t := range tables {
			if t == tableName {
				found = true
				break
			}
		}
		if !found {
			log.Infof("bigtable-idx: table %s does not yet exist. Creating it.", tableName)
			// table doesnt exist. So lets create it with all of the CF's we need.
			table := bigtable.TableConf{
				TableID: tableName,
				Families: map[string]bigtable.GCPolicy{
					COLUMN_FAMILY: bigtable.MaxVersionsPolicy(1),
				},
			}
			err := adminClient.CreateTableFromConf(ctx, &table)
			if err != nil {
				log.Fatalf("bigtable-idx: failed to create %s table. %s", tableName, err)
			}
		} else {
			log.Infof("bigtable-idx: table %s exists.", tableName)
			// table exists.  Lets make sure that it has all of the CF's we need.
			table, err := adminClient.TableInfo(ctx, tableName)
			if err != nil {
				log.Fatalf("bigtable-idx: failed to get tableInfo of %s. %s", tableName, err)
			}
			existingFamilies := make(map[string]string)
			for _, cf := range table.FamilyInfos {
				existingFamilies[cf.Name] = cf.GCPolicy
			}
			policy, ok := existingFamilies[COLUMN_FAMILY]
			if !ok {
				log.Infof("bigtable-idx: column family %s/%s does not exist. Creating it.", tableName, COLUMN_FAMILY)
				err = adminClient.CreateColumnFamily(ctx, tableName, COLUMN_FAMILY)
				if err != nil {
					log.Fatalf("btStore: failed to create cf %s/%s. %s", tableName, COLUMN_FAMILY, err)
				}
				err = adminClient.SetGCPolicy(ctx, tableName, COLUMN_FAMILY, bigtable.MaxVersionsPolicy(1))
				if err != nil {
					log.Fatalf("btStore: failed to set GCPolicy of %s/%s. %s", tableName, COLUMN_FAMILY, err)
				}
			} else if policy == "" {
				log.Infof("bigtable-idx: column family %s/%s exists but has no GCPolicy. Creating it.", tableName, COLUMN_FAMILY)
				err = adminClient.SetGCPolicy(ctx, tableName, COLUMN_FAMILY, bigtable.MaxVersionsPolicy(1))
				if err != nil {
					log.Fatalf("btStore: failed to set GCPolicy of %s/%s. %s", tableName, COLUMN_FAMILY, err)
				}
			}
		}
	}
	client, err := bigtable.NewClient(ctx, gcpProject, bigtableInstance)
	if err != nil {
		log.Fatalf("failed to create bigtable client. %s", err)
	}

	idx := &BigtableIdx{
		MemoryIdx: *memory.New(),
		client:    client,
		tbl:       client.Open(tableName),
		shutdown:  make(chan struct{}),
	}
	if updateBigTableIdx {
		idx.writeQueue = make(chan writeReq, writeQueueSize-writeMaxFlushSize)
	}
	updateInterval32 = uint32(updateInterval.Nanoseconds() / int64(time.Second))
	return idx
}

func (b *BigtableIdx) Init() error {
	log.Infof("initializing bigtable-idx. Project=%s, Instance=%s", gcpProject, bigtableInstance)
	if err := b.MemoryIdx.Init(); err != nil {
		return err
	}
	if updateBigTableIdx {
		b.wg.Add(writeConcurrency)
		for i := 0; i < writeConcurrency; i++ {
			go b.processWriteQueue()
		}
		log.Infof("bigtable-idx: started %d writeQueue handlers", writeConcurrency)
	}

	b.rebuildIndex()
	if maxStale > 0 {
		if pruneInterval == 0 {
			return fmt.Errorf("bigtable-idx: pruneInterval must be greater then 0")
		}
		b.wg.Add(1)
		go b.prune()
	}
	return nil
}

func (b *BigtableIdx) Stop() {
	b.MemoryIdx.Stop()
	close(b.shutdown)
	if updateBigTableIdx {
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

	if !updateBigTableIdx {
		statUpdateDuration.Value(time.Since(pre))
		return archive, oldPartition, inMemory
	}

	if inMemory {
		// bigtable uses partition ID in the key prefix, so an "update" that changes the partition for
		// an existing metricDef will just create a new row in the table and wont remove the old row.
		// So we need to explicitly delete the old entry.
		if oldPartition != partition {
			go func() {
				for {
					err := b.deleteRow(FormatRowKey(archive.Id, oldPartition))
					if err != nil {
						log.Errorf("bigtable-idx: failed to delete row. %s", err)
						time.Sleep(time.Second)
					} else {
						return
					}
				}
			}()
		}
		// check if we need to save to bigtable.
		now := uint32(time.Now().Unix())
		if archive.LastSave < (now - updateInterval32) {
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

	if !updateBigTableIdx {
		stat.Value(time.Since(pre))
		return archive, oldPartition, inMemory
	}

	if inMemory {
		// bigtable uses partition ID in the key prefix, so an "update" that changes the partition for
		// an existing metricDef will just create a new row in the table and wont remove the old row.
		// So we need to explicitly delete the old entry.
		if oldPartition != partition {
			go func() {
				for {
					err := b.deleteRow(FormatRowKey(archive.Id, oldPartition))
					if err != nil {
						log.Errorf("bigtable-idx: failed to delete row. %s", err)
						time.Sleep(time.Second)
					} else {
						return
					}
				}
			}()
		}
	}

	// check if we need to save to bigtable.
	now := uint32(time.Now().Unix())
	if archive.LastSave < (now - updateInterval32) {
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
	if archive.LastSave < (now - updateInterval32 - (updateInterval32 / 2)) {
		log.Debugf("bigtable-idx: updating def %s in index.", archive.MetricDefinition.Id)
		b.writeQueue <- writeReq{recvTime: time.Now(), def: &archive.MetricDefinition}
		archive.LastSave = now
		b.MemoryIdx.UpdateArchive(archive)
	} else {
		// perform a non-blocking write to the writeQueue. If the queue is full, then
		// this will fail and we wont update the LastSave timestamp. The next time
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

	var staleTs uint32
	if maxStale != 0 {
		staleTs = uint32(time.Now().Add(maxStale * -1).Unix())
	}
	num := 0
	var defs []schema.MetricDefinition
	for _, partition := range cluster.Manager.GetPartitions() {
		defs = b.LoadPartition(partition, defs[:0], staleTs)
		num += b.MemoryIdx.Load(defs)
	}

	log.Infof("bigtable-idx: Rebuilding Memory Index Complete. Imported %d. Took %s", num, time.Since(pre))
}

func (b *BigtableIdx) LoadPartition(partition int32, defs []schema.MetricDefinition, cutoff uint32) []schema.MetricDefinition {
	ctx := context.Background()
	rr := bigtable.PrefixRange(fmt.Sprintf("%d_", partition))
	defsBySeries := make(map[string][]schema.MetricDefinition)
	var marshalErr error
	err := b.tbl.ReadRows(ctx, rr, func(r bigtable.Row) bool {
		def := schema.MetricDefinition{}
		marshalErr = RowToSchema(r, &def)
		if marshalErr != nil {
			return false
		}
		log.Debugf("bigtable-idx: found def %+v", def)
		defsBySeries[def.Name] = append(defsBySeries[def.Name], def)
		return true
	}, bigtable.RowFilter(bigtable.FamilyFilter(COLUMN_FAMILY)))
	if err != nil {
		log.Fatalf("bigtable-idx: failed to load defs form Bigtable. %s", err)
	}
	if marshalErr != nil {
		log.Fatalf("bigtable-idx: failed to marshal row to metricDef. %s", marshalErr)
	}

LOOP:
	for series, defList := range defsBySeries {
		for _, def := range defList {
			if def.LastUpdate > int64(cutoff) {
				// add all defs for this series.
				defs = append(defs, defsBySeries[series]...)
				continue LOOP
			}
		}
		// all defs are stale
		delete(defsBySeries, series)
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
		success := false
		attempts := 0

		for !success {
			ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
			errs, err := b.tbl.ApplyBulk(ctx, rowKeys, mutations)
			cancel()
			if err != nil {
				log.Errorf("bigtable-idx: unable to apply writes to bigtable. %s", err)
				statQueryInsertFail.Add(len(rowKeys))
				if (attempts % 20) == 0 {
					log.Warnf("bigtable-idx: Failed to write %d defs to bigtable. they will be retried. %s", len(rowKeys), err)
				}
				sleepTime := 100 * attempts
				if sleepTime > 2000 {
					sleepTime = 2000
				}
				time.Sleep(time.Duration(sleepTime) * time.Millisecond)
				attempts++
			} else if len(errs) > 0 {
				var failedRowKeys []string
				var failedMutations []*bigtable.Mutation
				for i, err := range errs {
					if err != nil {
						failedRowKeys = append(failedRowKeys, rowKeys[i])
						failedMutations = append(failedMutations, mutations[i])
					}
				}
				rowKeys = failedRowKeys
				mutations = failedMutations
				log.Errorf("bigtable-idx: failed to write %d rows. %s", len(failedRowKeys), err)
				statQueryInsertFail.Add(len(failedRowKeys))
				sleepTime := 100 * attempts
				if sleepTime > 2000 {
					sleepTime = 2000
				}
				time.Sleep(time.Duration(sleepTime) * time.Millisecond)
				attempts++
			} else {
				success = true
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
				break LOOP
			}
			buffer = append(buffer, req)
			if len(buffer) >= writeMaxFlushSize {
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
	if updateBigTableIdx {
		for _, def := range defs {
			err = b.deleteDef(&def.MetricDefinition)
			if err != nil {
				break
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

func (b *BigtableIdx) Prune(oldest time.Time) ([]idx.Archive, error) {
	pre := time.Now()
	pruned, err := b.MemoryIdx.Prune(oldest)
	statPruneDuration.Value(time.Since(pre))
	return pruned, err
}

func (b *BigtableIdx) prune() {
	defer b.wg.Done()
	ticker := time.NewTicker(pruneInterval)
	for {
		select {
		case <-ticker.C:
			log.Debugf("bigtable-idx: pruning items from index that have not been seen for %s", maxStale.String())
			staleTs := time.Now().Add(maxStale * -1)
			_, err := b.Prune(staleTs)
			if err != nil {
				log.Errorf("bigtable-idx: prune error. %s", err)
			}
		case <-b.shutdown:
			return
		}
	}
}
