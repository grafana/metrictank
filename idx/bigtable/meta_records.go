package bigtable

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/bigtable"
	btUtils "github.com/grafana/metrictank/bigtable"
	"github.com/grafana/metrictank/expr/tagquery"
	"github.com/grafana/metrictank/idx/memory"
	"github.com/grafana/metrictank/idx/metatags"
	log "github.com/sirupsen/logrus"
)

var (
	errIdxUpdatesDisabled = fmt.Errorf("BigTable index updates are disabled")
)

// MetaRecordIdxConfig contains all the configurations related to storing the
// meta records in bigtable. this gets its own config struct to later make it
// easier to move all the meta record indexes into their own package hierarchy
// which will be independent of the indexes.
type MetaRecordIdxConfig struct {
	gcpProject        string
	bigtableInstance  string
	pollInterval      time.Duration
	pruneInterval     time.Duration
	pruneAge          time.Duration
	tableName         string
	batchTableName    string
	metaRecordCf      string
	metaRecordBatchCf string
	updateRecords     bool
	createCf          bool
}

type MetaRecordIdx struct {
	wg                   sync.WaitGroup
	shutdown             chan struct{}
	cfg                  MetaRecordIdxConfig
	status               metatags.MetaRecordStatusByOrg
	memoryIdx            memory.MemoryIndex
	client               *bigtable.Client
	adminClient          *bigtable.AdminClient
	metaRecordTable      *bigtable.Table
	metaRecordBatchTable *bigtable.Table
}

func NewBigTableMetaRecordIdx(cfg MetaRecordIdxConfig, memoryIdx memory.MemoryIndex) (*MetaRecordIdx, error) {
	m := &MetaRecordIdx{
		shutdown:  make(chan struct{}),
		cfg:       cfg,
		status:    metatags.NewMetaRecordStatusByOrg(),
		memoryIdx: memoryIdx,
	}

	ctx := context.Background()
	err := m.connect(ctx)
	if err != nil {
		return nil, err
	}

	err = m.initBare(ctx)
	if err != nil {
		return nil, err
	}

	return m, nil
}

func (b *MetaRecordIdx) connect(ctx context.Context) error {
	var err error
	b.client, err = bigtable.NewClient(ctx, b.cfg.gcpProject, b.cfg.bigtableInstance)
	if err != nil {
		log.Errorf("meta-record-idx: failed to create bigtable client: %s", err)
		return err
	}

	b.adminClient, err = bigtable.NewAdminClient(ctx, b.cfg.gcpProject, b.cfg.bigtableInstance)
	if err != nil {
		log.Errorf("meta-record-idx: failed to create bigtable admin client: %s", err)
	}

	return err
}

func (b *MetaRecordIdx) initBare(ctx context.Context) error {
	err := btUtils.EnsureTableExists(ctx, b.cfg.createCf, b.adminClient, "meta_records", map[string]bigtable.GCPolicy{
		b.cfg.metaRecordCf: bigtable.MaxVersionsPolicy(1),
	})
	if err != nil {
		return err
	}

	err = btUtils.EnsureTableExists(ctx, b.cfg.createCf, b.adminClient, "meta_record_batches", map[string]bigtable.GCPolicy{
		b.cfg.metaRecordBatchCf: bigtable.MaxVersionsPolicy(1),
	})
	if err != nil {
		return err
	}

	b.metaRecordBatchTable = b.client.Open(b.cfg.batchTableName)
	b.metaRecordTable = b.client.Open(b.cfg.tableName)

	return nil
}

func (b *MetaRecordIdx) start() {
	b.wg.Add(1)
	go b.pollBigtable()
	if b.cfg.updateRecords {
		b.wg.Add(1)
		go b.pruneMetaRecords()
	}
}

func (b *MetaRecordIdx) stop() {
	close(b.shutdown)
	b.wg.Wait()
	b.client.Close()
	b.adminClient.Close()
}

func (b *MetaRecordIdx) pollBigtable() {
	defer b.wg.Done()
	for {
		select {
		case <-b.shutdown:
			return
		default:
			b.loadMetaRecords()
			time.Sleep(b.cfg.pollInterval)
		}
	}
}

func (b *MetaRecordIdx) loadMetaRecords() {
	batches, err := b.readMetaRecordBatches()
	if err != nil {
		log.Errorf("meta-record-idx: Failed to load meta records from idx: %s", err)
		return
	}

	toLoad := make(map[uint32]metatags.UUID)
	for batchId, properties := range batches {
		load, batchId := b.status.Update(properties.orgId, batchId, uint64(properties.createdAt), uint64(properties.lastUpdate))
		if load {
			toLoad[properties.orgId] = batchId
		}
	}

	for orgId, batchId := range toLoad {
		log.Infof("meta-record-idx: Loading meta record batch %s of org %d", batchId.String(), orgId)

		records, err := b.readMetaRecordsOfBatch(orgId, batchId)
		if err != nil {
			log.Errorf("meta-record-idx: Failed to read meta records of batch (%d/%s): %s", orgId, batchId.String(), err)
			continue
		}

		err = b.memoryIdx.MetaTagRecordSwap(orgId, records)
		if err != nil {
			log.Errorf("meta-record-idx: Error when trying to swap batch of meta records in memory index (%d/%s): %s", orgId, batchId.String(), err)
			continue
		}
	}
}

func (b *MetaRecordIdx) pruneMetaRecords() {
	defer b.wg.Done()
	for {
		select {
		case <-b.shutdown:
			return
		default:
			time.Sleep(b.cfg.pruneInterval)

			batches, err := b.readMetaRecordBatches()
			if err != nil {
				log.Errorf("meta-record-idx: Skipping pruning because couldn't load meta record batches: %s", err)
				continue
			}
			for batchId, properties := range batches {
				now := time.Now().Unix()
				if uint64(now)-uint64(b.cfg.pruneAge.Seconds()) <= uint64(properties.createdAt/1000) {
					continue
				}

				currentBatchId, _, _ := b.status.GetStatus(properties.orgId)
				if batchId != currentBatchId {
					err := b.pruneBatch(properties.orgId, batchId)
					if err != nil {
						log.Errorf("meta-record-idx: Error when pruning batch %d/%s: %s", properties.orgId, batchId.String(), err)
					}
				}
			}
		}
	}
}

type metaRecordBatches map[metatags.UUID]metaRecordBatchProperties
type metaRecordBatchProperties struct {
	orgId      uint32
	createdAt  int64
	lastUpdate int64
}

func (b *MetaRecordIdx) readMetaRecordBatches() (metaRecordBatches, error) {
	res := make(metaRecordBatches)

	ctx := context.Background()
	err := b.metaRecordBatchTable.ReadRows(ctx, bigtable.InfiniteRange(""), func(row bigtable.Row) bool {
		var uuid metatags.UUID
		var err error
		var properties metaRecordBatchProperties
		properties.orgId, uuid, err = decodeMetaRecordBatchRowKey(row.Key())
		if err != nil {
			log.Errorf("meta-record-idx: Failed to decode meta record batch row key %q: %s", row.Key(), err)
			return false
		}

		columns, ok := row[b.cfg.metaRecordBatchCf]
		if !ok {
			log.Errorf("meta-record-idx: Row from meta record batch table was missing expected column family %s", b.cfg.metaRecordBatchCf)
			return false
		}

		for _, col := range columns {
			switch strings.SplitN(col.Column, ":", 2)[1] {
			case "lastupdate":
				properties.lastUpdate, err = binary.ReadVarint(bytes.NewReader(col.Value))
				if err != nil {
					log.Errorf("meta-record-idx: Row from meta record batch table contained invalid lastupdate value %x: %s", col.Value, err)
					return false
				}
			case "createdat":
				properties.createdAt, err = binary.ReadVarint(bytes.NewReader(col.Value))
				if err != nil {
					log.Errorf("meta-record-idx: Row from meta record batch table contained invalid createdat value %x: %s", col.Value, err)
					return false
				}
			}
		}

		res[uuid] = properties
		return true
	}, bigtable.RowFilter(bigtable.ChainFilters(bigtable.FamilyFilter(b.cfg.metaRecordBatchCf), bigtable.LatestNFilter(1))))

	if err != nil {
		log.Errorf("meta-record-idx: Failed to load meta record batches: %s", err)
		return nil, err
	}

	return res, nil
}

func (b *MetaRecordIdx) readMetaRecordsOfBatch(orgId uint32, batchId metatags.UUID) ([]tagquery.MetaTagRecord, error) {
	var res []tagquery.MetaTagRecord
	ctx := context.Background()
	err := b.metaRecordTable.ReadRows(ctx, bigtable.PrefixRange(formatMetaRecordRowKey(orgId, batchId, "")), func(row bigtable.Row) bool {
		var record tagquery.MetaTagRecord
		var err error
		_, _, record.Expressions, err = decodeMetaRecordRowKey(row.Key())
		if err != nil {
			log.Errorf("meta-record-idx: Failed to decode meta record row key %q: %s", row.Key(), err)
			return false
		}

		columns, ok := row[b.cfg.metaRecordCf]
		if !ok {
			log.Errorf("meta-record-idx: Row from meta record table was missing expected column family %s", b.cfg.metaRecordCf)
			return false
		}

		for _, col := range columns {
			switch strings.SplitN(col.Column, ":", 2)[1] {
			case "metatags":
				err = record.MetaTags.UnmarshalJSON([]byte(col.Value))
				if err != nil {
					log.Errorf("meta-record-idx: Row from meta record table contained invalid metatags value %q: %s", col.Value, err)
					return false
				}
			}
		}

		res = append(res, record)
		return true
	}, bigtable.RowFilter(bigtable.ChainFilters(bigtable.FamilyFilter(b.cfg.metaRecordCf), bigtable.LatestNFilter(1))))

	if err != nil {
		log.Errorf("meta-record-idx: Failed to load meta records of batch (%d/%s):%s", orgId, batchId.String(), err)
		return nil, err
	}

	return res, nil
}

func (b *MetaRecordIdx) pruneBatch(orgId uint32, batchId metatags.UUID) error {
	// unfortunately dropping a row range requires using the admin client
	err := b.adminClient.DropRowRange(context.Background(), b.cfg.tableName, formatMetaRecordRowKey(orgId, batchId, ""))
	if err != nil {
		log.Errorf("meta-record-idx: Failed to drop row range by prefix %s: %s", formatMetaRecordRowKey(orgId, batchId, ""), err)
	}

	mut := bigtable.NewMutation()
	mut.DeleteRow()
	err = b.metaRecordBatchTable.Apply(context.Background(), formatMetaRecordBatchRowKey(orgId, batchId), mut)
	if err != nil {
		log.Errorf("meta-record-idx: Failed to delete meta record batch (%d/%s): %s", orgId, batchId.String(), err)
	}

	return err
}

func (b *MetaRecordIdx) MetaTagRecordUpsert(orgId uint32, record tagquery.MetaTagRecord) error {
	if !memory.MetaTagSupport || !memory.TagSupport {
		return metatags.ErrMetaTagSupportDisabled
	}
	if !b.cfg.updateRecords {
		return errIdxUpdatesDisabled
	}

	batchId, _, _ := b.status.GetStatus(orgId)

	// if a record has no meta tags associated with it, then we delete it
	var err error
	if len(record.MetaTags) > 0 {
		err = b.persistMetaRecord(orgId, batchId, record)
	} else {
		err = b.deleteMetaRecord(orgId, batchId, record)
	}

	if err != nil {
		log.Errorf("meta-record-idx: Failed to update meta records in bigtable: %s", err)
		return fmt.Errorf("Failed to update bigtable: %s", err)
	}

	err = b.markMetaRecordBatchUpdated(orgId, batchId)
	if err != nil {
		log.Errorf("meta-record-idx: Failed to update meta records in bigtable: %s", err)
		return fmt.Errorf("Failed to update bigtable: %s", err)
	}

	return nil
}

func formatMetaRecordBatchRowKey(orgId uint32, batchId metatags.UUID) string {
	return strconv.Itoa(int(orgId)) + "_" + batchId.String()
}

func decodeMetaRecordBatchRowKey(key string) (uint32, metatags.UUID, error) {
	var uuid metatags.UUID
	parts := strings.SplitN(key, "_", 2)
	if len(parts) < 2 {
		return 0, uuid, fmt.Errorf("Invalid row key format: %q", key)
	}
	orgId, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, uuid, fmt.Errorf("Invalid org id %q in row key: %q", parts[0], key)
	}
	uuid, err = metatags.ParseUUID(parts[1])
	if err != nil {
		return 0, uuid, fmt.Errorf("Failed to parse uuid %q in row key %q: %s", uuid, key, err)
	}

	return uint32(orgId), uuid, nil
}

func (b *MetaRecordIdx) markMetaRecordBatchUpdated(orgId uint32, batchId metatags.UUID) error {
	now := make([]byte, 8)
	binary.PutVarint(now, time.Now().UnixNano()/1000000)
	mut := bigtable.NewMutation()
	mut.Set(b.cfg.metaRecordBatchCf, "lastupdate", bigtable.Now(), now)

	if batchId == metatags.DefaultBatchId {
		// if the current batch id is the default batch id, then this batch has most likely not
		// been persisted yet and we're going to persist it for the first time now. since this
		// is the default batch which always exists, we set "createdat" to 0.
		mut.Set(b.cfg.metaRecordBatchCf, "createdat", bigtable.Now(), make([]byte, 8))
	}

	err := b.metaRecordBatchTable.Apply(context.Background(), formatMetaRecordBatchRowKey(orgId, batchId), mut)
	if err != nil {
		log.Errorf("meta-record-idx: Failed to mark meta record batch (%d/%s) as updated: %s", orgId, batchId.String(), err)
	}
	return err
}

func (b *MetaRecordIdx) MetaTagRecordSwap(orgId uint32, records []tagquery.MetaTagRecord) error {
	if !memory.MetaTagSupport || !memory.TagSupport {
		return metatags.ErrMetaTagSupportDisabled
	}
	if !b.cfg.updateRecords {
		return errIdxUpdatesDisabled
	}

	newBatchId, err := metatags.RandomUUID()
	if err != nil {
		return fmt.Errorf("Failed to generate new batch id")
	}

	var expressions, metaTags []byte
	rowKeys := make([]string, len(records))
	muts := make([]*bigtable.Mutation, len(records))
	for i, record := range records {
		record.Expressions.Sort()
		expressions, err = record.Expressions.MarshalJSON()
		if err != nil {
			return fmt.Errorf("Failed to marshal expressions: %s", err)
		}
		record.MetaTags.Sort()
		metaTags, err = record.MetaTags.MarshalJSON()
		if err != nil {
			return fmt.Errorf("Failed to marshal meta tags: %s", err)
		}

		rowKeys[i] = formatMetaRecordRowKey(orgId, newBatchId, string(expressions))
		muts[i].Set(b.cfg.metaRecordCf, "metatags", bigtable.Now(), metaTags)
	}

	select {
	case <-b.shutdown:
		return fmt.Errorf("meta-record-idx: Shutting down")
	default:
	}

	errs, err := b.metaRecordTable.ApplyBulk(context.Background(), rowKeys, muts)
	if err != nil {
		log.Errorf("meta-record-idx: Failed to apply meta records in bulk: %s", err)
		return err
	} else if len(errs) > 0 {
		for _, err = range errs {
			if err != nil {
				log.Errorf("meta-record-idx: One or multiple errors when storing %d records, first error: %s", len(rowKeys), err)
				return err
			}
		}
	}

	select {
	case <-b.shutdown:
		return fmt.Errorf("meta-record-idx: Shutting down")
	default:
	}

	return b.createNewBatch(orgId, newBatchId)
}

func (b *MetaRecordIdx) createNewBatch(orgId uint32, batchId metatags.UUID) error {
	now := make([]byte, 8)
	binary.PutVarint(now, time.Now().UnixNano()/1000000)
	mut := bigtable.NewMutation()
	mut.Set(b.cfg.metaRecordBatchCf, "lastupdate", bigtable.Now(), now)
	mut.Set(b.cfg.metaRecordBatchCf, "createdat", bigtable.Now(), now)

	rowKey := formatMetaRecordBatchRowKey(orgId, batchId)
	err := b.metaRecordBatchTable.Apply(context.Background(), rowKey, mut)
	if err != nil {
		log.Errorf("bt-meta-record-idx: Failed to create new batch (%d/%s): %s", orgId, batchId.String(), err)
	}
	return err
}

func formatMetaRecordRowKey(orgId uint32, batchId metatags.UUID, expressions string) string {
	return strconv.Itoa(int(orgId)) + "_" + batchId.String() + "_" + expressions
}

func decodeMetaRecordRowKey(key string) (uint32, metatags.UUID, tagquery.Expressions, error) {
	parts := strings.SplitN(key, "_", 3)
	if len(parts) < 3 {
		return 0, metatags.UUID{}, tagquery.Expressions{}, fmt.Errorf("Invalid row key format: %q", key)
	}

	orgId, err := strconv.Atoi(parts[0])
	if err != nil {
		return 0, metatags.UUID{}, tagquery.Expressions{}, fmt.Errorf("Invalid org id in key %q: %q", key, parts[0])
	}

	uuid, err := metatags.ParseUUID(parts[1])
	if err != nil {
		return 0, metatags.UUID{}, tagquery.Expressions{}, fmt.Errorf("Failed to parse uuid %q in key %q: %s", parts[1], key, err)
	}

	var expressions tagquery.Expressions
	err = expressions.UnmarshalJSON([]byte(parts[2]))
	if err != nil {
		return 0, metatags.UUID{}, expressions, fmt.Errorf("Failed to parse expressions string %q in key %q: %s", parts[2], key, err)
	}

	return uint32(orgId), uuid, expressions, nil
}

func (b *MetaRecordIdx) persistMetaRecord(orgId uint32, batchId metatags.UUID, record tagquery.MetaTagRecord) error {
	expressions, err := record.Expressions.MarshalJSON()
	if err != nil {
		return fmt.Errorf("Failed to marshal expressions: %s", err)
	}
	metaTags, err := record.MetaTags.MarshalJSON()
	if err != nil {
		return fmt.Errorf("Failed to marshal meta tags: %s", err)
	}

	mut := bigtable.NewMutation()
	mut.Set(b.cfg.metaRecordCf, "metatags", bigtable.Now(), metaTags)

	err = b.metaRecordTable.Apply(context.Background(), formatMetaRecordRowKey(orgId, batchId, string(expressions)), mut)
	if err != nil {
		log.Errorf("meta-record-idx: Failed to persist meta record (%d/%s/%s/%s): %s", orgId, batchId.String(), expressions, metaTags, err)
	}
	return err
}

func (b *MetaRecordIdx) deleteMetaRecord(orgId uint32, batchId metatags.UUID, record tagquery.MetaTagRecord) error {
	expressions, err := record.Expressions.MarshalJSON()
	if err != nil {
		return fmt.Errorf("Failed to marshal record expressions: %s", err)
	}

	mut := bigtable.NewMutation()
	mut.DeleteRow()

	err = b.metaRecordTable.Apply(context.Background(), formatMetaRecordRowKey(orgId, batchId, string(expressions)), mut)
	if err != nil {
		log.Errorf("meta-record-idx: Failed to delete meta record (%d/%s/%s): %s", orgId, batchId.String(), expressions, err)
	}
	return err
}
