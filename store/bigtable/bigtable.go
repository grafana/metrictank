package bigtable

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"cloud.google.com/go/bigtable"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/mdata/chunk"
	"github.com/grafana/metrictank/stats"
	"github.com/grafana/metrictank/tracing"
	"github.com/grafana/metrictank/util"
	opentracing "github.com/opentracing/opentracing-go"
	tags "github.com/opentracing/opentracing-go/ext"
	"github.com/raintank/dur"
	"github.com/raintank/schema"
	log "github.com/sirupsen/logrus"
)

const Month_sec uint32 = 60 * 60 * 24 * 28

var (
	errChunkTooSmall  = errors.New("impossibly small chunk in bigtable")
	errStartBeforeEnd = errors.New("start must be before end.")
	errCtxCanceled    = errors.New("context canceled")

	// metric store.bigtable.get.exec is the duration of getting from bigtable store
	btblGetExecDuration = stats.NewLatencyHistogram15s32("store.bigtable.get.exec")
	// metric store.bigtable.get.wait is the duration of the get spent in the queue
	btblGetWaitDuration = stats.NewLatencyHistogram12h32("store.bigtable.get.wait")
	// metric store.bigtable.put.exec is the duration of putting in bigtable store
	btblPutExecDuration = stats.NewLatencyHistogram15s32("store.bigtable.put.exec")
	// metric store.bigtable.put.wait is the duration of a put in the wait queue
	btblPutWaitDuration = stats.NewLatencyHistogram12h32("store.bigtable.put.wait")
	// metric store.bigtable.put.bytes is the number of chunk bytes saved in each bulkApply
	btblPutBytes = stats.NewMeter32("store.bigtable.put.bytes", true)
	// metric store.bigtable.get.error is the count of reads that failed
	btblReadError = stats.NewCounter32("store.bigtable.get.error")

	// metric store.bigtable.chunks_per_row is how many chunks are retrieved per row in get queries
	btblChunksPerRow = stats.NewMeter32("store.bigtable.chunks_per_row", false)
	// metric store.bigtable.rows_per_response is how many rows come per get response
	btblRowsPerResponse = stats.NewMeter32("store.bigtable.rows_per_response", false)

	// metric store.bigtable.chunk_operations.save_ok is counter of successful saves
	chunkSaveOk = stats.NewCounter32("store.bigtable.chunk_operations.save_ok")
	// metric store.bigtable.chunk_operations.save_fail is counter of failed saves
	chunkSaveFail = stats.NewCounter32("store.bigtable.chunk_operations.save_fail")
	// metric store.bigtable.chunk_size.at_save is the sizes of chunks seen when saving them
	chunkSizeAtSave = stats.NewMeter32("store.bigtable.chunk_size.at_save", true)
	// metric store.bigtable.chunk_size.at_load is the sizes of chunks seen when loading them
	chunkSizeAtLoad = stats.NewMeter32("store.bigtable.chunk_size.at_load", true)
)

func formatRowKey(key schema.AMKey, ts uint32) string {
	return fmt.Sprintf("%s_%d", key.MKey.String(), ts/Month_sec)
}

func formatFamily(ttl uint32) string {
	return dur.FormatDuration(ttl)
}

func PrepareChunkData(span uint32, data []byte) []byte {
	chunkSizeAtSave.Value(len(data))
	version := chunk.FormatStandardGoTszWithSpan
	buf := new(bytes.Buffer)
	binary.Write(buf, binary.LittleEndian, version)

	spanCode, ok := chunk.RevChunkSpans[span]
	if !ok {
		// it's probably better to panic than to persist the chunk with a wrong length
		panic(fmt.Sprintf("Chunk span invalid: %d", span))
	}
	binary.Write(buf, binary.LittleEndian, spanCode)
	buf.Write(data)
	return buf.Bytes()
}

func mutationFromWriteRequest(cwr *mdata.ChunkWriteRequest) (*bigtable.Mutation, int) {
	mut := bigtable.NewMutation()
	family := formatFamily(cwr.TTL)
	column := "raw"
	if cwr.Key.Archive > 0 {
		column = cwr.Key.Archive.String()
	}
	value := PrepareChunkData(cwr.Span, cwr.Chunk.Series.Bytes())
	chunkSizeAtSave.Value(len(value))
	mut.Set(family, column, bigtable.Timestamp(int64(cwr.Chunk.T0)*1e6), value)
	return mut, len(value)
}

type Store struct {
	tbl              *bigtable.Table
	client           *bigtable.Client
	writeQueues      []chan *mdata.ChunkWriteRequest
	writeQueueMeters []*stats.Range32
	readLimiter      util.Limiter
	shutdown         chan struct{}
	wg               sync.WaitGroup
	tracer           opentracing.Tracer
	cfg              *StoreConfig
}

func NewStore(cfg *StoreConfig, ttls []uint32, schemaMaxChunkSpan uint32) (*Store, error) {
	// Hopefully the caller has already validated their config, but just in case,
	// lets make sure.
	if err := cfg.Validate(schemaMaxChunkSpan); err != nil {
		return nil, err
	}
	ctx := context.Background()
	if cfg.CreateCF {
		adminClient, err := bigtable.NewAdminClient(ctx, cfg.GcpProject, cfg.BigtableInstance)
		if err != nil {
			return nil, fmt.Errorf("btStore: failed to create bigtable admin client. %s", err)
		}
		tables, err := adminClient.Tables(ctx)
		if err != nil {
			return nil, fmt.Errorf("btStore: failed to list tables. %s", err)
		}
		found := false
		for _, t := range tables {
			if t == cfg.TableName {
				found = true
				break
			}
		}
		if !found {
			log.Infof("btStore: table %s does not exist. Creating it.", cfg.TableName)
			table := bigtable.TableConf{
				TableID:  cfg.TableName,
				Families: make(map[string]bigtable.GCPolicy),
			}
			for _, ttl := range ttls {
				table.Families[formatFamily(ttl)] = bigtable.MaxAgePolicy(time.Duration(ttl) * time.Second)
			}
			err := adminClient.CreateTableFromConf(ctx, &table)
			if err != nil {
				return nil, fmt.Errorf("btStore: failed to create %s table. %s", cfg.TableName, err)
			}
		} else {
			log.Infof("btStore: table %s exists.", cfg.TableName)
			// table exists.  Lets make sure that it has all of the CF's we need.
			table, err := adminClient.TableInfo(ctx, cfg.TableName)
			if err != nil {
				return nil, fmt.Errorf("btStore: failed to get tableInfo of %s. %s", cfg.TableName, err)
			}
			existingFamilies := make(map[string]string)
			for _, cf := range table.FamilyInfos {
				existingFamilies[cf.Name] = cf.GCPolicy
			}
			for _, ttl := range ttls {
				policy, ok := existingFamilies[formatFamily(ttl)]
				if !ok {
					log.Infof("btStore: column family %s/%s does not exist. creating it", cfg.TableName, formatFamily(ttl))
					err = adminClient.CreateColumnFamily(ctx, cfg.TableName, formatFamily(ttl))
					if err != nil {
						return nil, fmt.Errorf("btStore: failed to create cf %s/%s. %s", cfg.TableName, formatFamily(ttl), err)
					}
					err = adminClient.SetGCPolicy(ctx, cfg.TableName, formatFamily(ttl), bigtable.MaxAgePolicy(time.Duration(ttl)*time.Second))
					if err != nil {
						return nil, fmt.Errorf("btStore: failed to set GCPolicy of %s/%s. %s", cfg.TableName, formatFamily(ttl), err)
					}
				} else if policy == "" {
					log.Infof("btStore: column family %s/%s exists but has no GCPolicy. creating it", cfg.TableName, formatFamily(ttl))
					err = adminClient.SetGCPolicy(ctx, cfg.TableName, formatFamily(ttl), bigtable.MaxAgePolicy(time.Duration(ttl)*time.Second))
					if err != nil {
						return nil, fmt.Errorf("btStore: failed to set GCPolicy of %s/%s. %s", cfg.TableName, formatFamily(ttl), err)
					}
				}
			}
		}
	}

	client, err := bigtable.NewClient(ctx, cfg.GcpProject, cfg.BigtableInstance)
	if err != nil {
		return nil, fmt.Errorf("btStore: failed to create bigtable client. %s", err)
	}

	s := &Store{
		client:           client,
		tbl:              client.Open(cfg.TableName),
		shutdown:         make(chan struct{}),
		writeQueues:      make([]chan *mdata.ChunkWriteRequest, cfg.WriteConcurrency),
		writeQueueMeters: make([]*stats.Range32, cfg.WriteConcurrency),
		readLimiter:      util.NewLimiter(cfg.ReadConcurrency),
		cfg:              cfg,
	}
	s.wg.Add(cfg.WriteConcurrency)
	for i := 0; i < cfg.WriteConcurrency; i++ {
		// Each processWriteQueue thread uses a channel and a buffer for queuing unwritten chunks.
		// In total, each processWriteQueue thread should not have more then "write-queue-size" chunks
		// that are queued.  To ensure this, set the channel size to "write-queue-size" - "write-max-flush-size"
		s.writeQueues[i] = make(chan *mdata.ChunkWriteRequest, cfg.WriteQueueSize-cfg.WriteMaxFlushSize)
		s.writeQueueMeters[i] = stats.NewRange32(fmt.Sprintf("store.bigtable.write_queue.%d.items", i+1))
		go s.processWriteQueue(s.writeQueues[i], s.writeQueueMeters[i])
	}

	return s, nil
}

func (s *Store) Stop() {
	close(s.shutdown)
	s.wg.Wait()
	err := s.client.Close()
	if err != nil {
		log.Errorf("btStore: error closing bigtable client. %s", err)
	}
}

func (s *Store) Add(cwr *mdata.ChunkWriteRequest) {
	sum := int(cwr.Key.MKey.Org)
	for _, b := range cwr.Key.MKey.Key {
		sum += int(b)
	}
	which := sum % len(s.writeQueues)
	s.writeQueues[which] <- cwr
}

func (s *Store) processWriteQueue(queue chan *mdata.ChunkWriteRequest, meter *stats.Range32) {
	defer s.wg.Done()
	// monitor the queue length.  We use a separate goroutine so that monitoring will still
	// occur even if reading from the queue blocks.
	go func() {
		ticker := time.NewTicker(time.Second)
		for {
			select {
			case <-ticker.C:
				meter.Value(len(queue))
			case <-s.shutdown:
				ticker.Stop()
				return
			}
		}
	}()

	timer := time.NewTimer(time.Second)
	buf := make([]*mdata.ChunkWriteRequest, 0)
	n := 0
	flush := func() {
		bytesPerFlush := 0
		log.Debugf("btStore: starting to save %d chunks", len(buf))
		rowKeys := make([]string, len(buf))
		muts := make([]*bigtable.Mutation, len(buf))
		for i, cwr := range buf {
			rowKeys[i] = formatRowKey(cwr.Key, cwr.Chunk.T0)
			muts[i], n = mutationFromWriteRequest(cwr)
			//record how long the chunk waited in the queue before we attempted to save to bigtable
			btblPutWaitDuration.Value(time.Now().Sub(cwr.Timestamp))
			bytesPerFlush += n
		}
		btblPutBytes.Value(bytesPerFlush)
		success := false
		attempts := 0
		for !success {
			pre := time.Now()
			ctx, cancel := context.WithTimeout(context.Background(), s.cfg.WriteTimeout)
			errs, err := s.tbl.ApplyBulk(ctx, rowKeys, muts)
			cancel()
			btblPutExecDuration.Value(time.Since(pre))
			if err != nil {
				// all chunks in the batch failed to be written.
				log.Errorf("btStore: unable to apply writes to bigtable. %s", err)
				chunkSaveFail.Add(len(rowKeys))
				if (attempts % 20) == 0 {
					log.Warnf("btStore: Failed to write %d chunks to bigtable. they will be retried. %s", len(rowKeys), err)
				}
				sleepTime := 100 * attempts
				if sleepTime > 2000 {
					sleepTime = 2000
				}
				time.Sleep(time.Duration(sleepTime) * time.Millisecond)
				attempts++
			} else if len(errs) > 0 {
				// only some chunks in the batch failed to get written.
				failedRowKeys := make([]string, 0)
				failedMutations := make([]*bigtable.Mutation, 0)
				retryBuf := make([]*mdata.ChunkWriteRequest, 0)
				for i, err := range errs {
					if err != nil {
						failedRowKeys = append(failedRowKeys, rowKeys[i])
						failedMutations = append(failedMutations, muts[i])
						retryBuf = append(retryBuf, buf[i])
					} else {
						buf[i].Metric.SyncChunkSaveState(buf[i].Chunk.T0)
						mdata.SendPersistMessage(buf[i].Key.String(), buf[i].Chunk.T0)
						log.Debugf("btStore: save complete. %s:%d %v", buf[i].Key, buf[i].Chunk.T0, buf[i].Chunk)
						chunkSaveOk.Inc()
					}
				}
				rowKeys = failedRowKeys
				muts = failedMutations
				buf = retryBuf
				log.Errorf("btStore: failed to write %d rows. %s", len(failedRowKeys), err)
				chunkSaveFail.Add(len(failedRowKeys))
				sleepTime := 100 * attempts
				if sleepTime > 2000 {
					sleepTime = 2000
				}
				time.Sleep(time.Duration(sleepTime) * time.Millisecond)
				attempts++
			} else {
				success = true
				chunkSaveOk.Add(len(rowKeys))
				log.Debugf("btStore: %d chunks saved to bigtable.", len(rowKeys))
				for _, cwr := range buf {
					cwr.Metric.SyncChunkSaveState(cwr.Chunk.T0)
					mdata.SendPersistMessage(cwr.Key.String(), cwr.Chunk.T0)
					log.Debugf("btStore: save complete. %s:%d %v", cwr.Key.String(), cwr.Chunk.T0, cwr.Chunk)
				}
			}
		}
		buf = buf[:0]
	}
	for {
		select {
		case <-timer.C:
			timer.Reset(time.Second)
			if len(buf) > 0 {
				flush()
			}
		case cwr := <-queue:
			buf = append(buf, cwr)
			if len(buf) >= s.cfg.WriteMaxFlushSize {
				// make sure the timer hasn't already fired. If it has we read
				// from the chan and consume the event.
				if !timer.Stop() {
					<-timer.C
				}
				timer.Reset(time.Second)
				flush()
			}
		case <-s.shutdown:
			return
		}
	}
}

func (s *Store) SetTracer(t opentracing.Tracer) {
	s.tracer = t
}

type SortedIterGen []chunk.IterGen

func (a SortedIterGen) Len() int           { return len(a) }
func (a SortedIterGen) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a SortedIterGen) Less(i, j int) bool { return a[i].Ts < a[j].Ts }

// Basic search of bigtable for data chunks
// start inclusive, end exclusive
func (s *Store) Search(ctx context.Context, key schema.AMKey, ttl, start, end uint32) ([]chunk.IterGen, error) {
	log.Debugf("btStore: fetching chunks for metric %s in range %d %d", key, start, end)
	_, span := tracing.NewSpan(ctx, s.tracer, "BigtableStore.Search")
	defer span.Finish()
	tags.SpanKindRPCClient.Set(span)
	tags.PeerService.Set(span, "bigtable")

	itgens := make([]chunk.IterGen, 0)
	if start > end {
		tracing.Failure(span)
		tracing.Error(span, errStartBeforeEnd)
		return itgens, errStartBeforeEnd
	}

	// limit number of inflight requests
	log.Debugf("btStore: waiting for Search slot. len=%d cap=%d", len(s.readLimiter), cap(s.readLimiter))
	pre := time.Now()
	if !s.readLimiter.Acquire(ctx) {
		return itgens, errCtxCanceled
	}
	log.Debug("btStore: acquired a Search slot")
	btblGetWaitDuration.Value(time.Since(pre))

	startMonth := start - (start % Month_sec)       // starting row has to be at, or before, requested start
	endMonth := (end - 1) - ((end - 1) % Month_sec) // ending row has to include the last point we might need (end-1)

	// unfortunately in the database we only have the t0's of all chunks.
	// this means we can easily make sure to include the correct last chunk (just query for a t0 < end, the last chunk will contain the last needed data)
	// but it becomes hard to find which should be the first chunk to include. we can't just query for start <= t0 because than we will miss some data at
	// the beginning. We can't assume we know the chunkSpan so we can't just calculate the t0 >= (start - <some-predefined-number>) because ChunkSpans
	// may change over time.
	// We effectively need all chunks with a t0 > start, as well as the last chunk with a t0 <= start.
	// Bigtable doesn't allow us to fetch the most recent chunk older then start, so instead we just fetch from (start - maxChunkSpan) to enure we get the data. This
	// will usually result in more data being fetched then is needed, but that is an acceptable tradeoff.
	adjustedStart := start - uint32(s.cfg.MaxChunkSpan.Seconds())

	// chunkspans are aligned with Month_sec.  If start is > startMonth then the T0 of the chunk that start is in will also be >= startMonth.
	if adjustedStart < startMonth {
		adjustedStart = startMonth
	}
	agg := "raw"
	if key.Archive > 0 {
		agg = key.Archive.String()
	}
	// filter the results to just the agg method (Eg raw, min_60, max_1800, etc..) and the timerange we want.
	// we fetch all columnFamilies (which are the different TTLs).  Typically there will be only one columnFamily
	// that has data, unless the TTL of the agg has changed.  In which case we want all columnFamilies anyway.
	filter := bigtable.ChainFilters(
		bigtable.ColumnFilter(agg),
		bigtable.TimestampRangeFilterMicros(bigtable.Timestamp(int64(adjustedStart)*1e6), bigtable.Timestamp(int64(end)*1e6)),
	)

	firstRow := formatRowKey(key, startMonth)
	lastRow := formatRowKey(key, endMonth+Month_sec) // bigtable.RowRange is start inclusive, end exclusive
	rr := bigtable.NewRange(firstRow, lastRow)
	var err error
	rowCount := 0
	pre = time.Now()
	queryCtx, cancel := context.WithTimeout(ctx, s.cfg.ReadTimeout)
	reqErr := s.tbl.ReadRows(queryCtx, rr, func(row bigtable.Row) bool {
		rowCount++
		chunks := 0
		var itgen *chunk.IterGen
		for _, items := range row {
			for _, rItem := range items {
				chunkSizeAtLoad.Value(len(rItem.Value))
				if len(rItem.Value) < 2 {
					log.Errorf("btStore: bigtable readRows error. %s", err)
					err = errChunkTooSmall
					return false
				}
				itgen, err = chunk.NewGen(rItem.Value, uint32(rItem.Timestamp/1e6))
				if err != nil {
					log.Errorf("btStore: unable to create chunk from bytes. %s", err)
					return false
				}
				chunks++

				// This function is called serially so we don't need synchronization here
				itgens = append(itgens, *itgen)
			}
		}
		btblChunksPerRow.Value(chunks)

		return true
	}, bigtable.RowFilter(filter))
	cancel()
	btblRowsPerResponse.Value(rowCount)
	btblGetExecDuration.Value(time.Since(pre))

	// free a slot in the readLimiter
	s.readLimiter.Release()

	if reqErr != nil {
		log.Errorf("btStore: bigtable readRows error. %s", reqErr)
		err = reqErr
	}
	if err != nil {
		tracing.Failure(span)
		tracing.Error(span, err)
		btblReadError.Inc()
	}
	// TODO: do we need to ensure that itgens is sorted by chunk T0?
	sort.Sort(SortedIterGen(itgens))
	return itgens, err
}
