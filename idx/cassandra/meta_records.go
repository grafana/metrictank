package cassandra

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/grafana/metrictank/expr/tagquery"
	"github.com/grafana/metrictank/idx/memory"
	log "github.com/sirupsen/logrus"
)

var (
	// try executing each query 10 times
	// when a query fails we retry 9 times with the following sleep times in-between
	// 100ms, 200ms, 400ms, 800ms, 1.6s, 3.2s, 6.4s, 12.8s, 20s
	// the total time to fail is 45.5s, which is less than the default http timeout
	metaRecordRetryPolicy = gocql.ExponentialBackoffRetryPolicy{
		NumRetries: 9,
		Min:        time.Millisecond * time.Duration(100),
		Max:        time.Second * time.Duration(20),
	}

	// this batch id is used if we handle an upsert request for an org that has
	// no current batch
	defaultBatchId = gocql.UUID{}

	errIdxUpdatesDisabled     = fmt.Errorf("Cassandra index updates are disabled")
	errMetaTagSupportDisabled = fmt.Errorf("Meta tag support is not enabled")
)

type metaRecordStatusByOrg struct {
	byOrg map[uint32]metaRecordStatus
}

func newMetaRecordStatusByOrg() metaRecordStatusByOrg {
	return metaRecordStatusByOrg{byOrg: make(map[uint32]metaRecordStatus)}
}

type metaRecordStatus struct {
	batchId    gocql.UUID
	createdAt  uint64
	lastUpdate uint64
}

// update takes the properties describing a batch of meta records and updates its internal status if necessary
// it returns a boolean indicating whether a reload of the meta records is necessary and
// if it is then the second returned value is the batch id that needs to be loaded
func (m *metaRecordStatusByOrg) update(orgId uint32, newBatch gocql.UUID, newCreatedAt, newLastUpdate uint64) (bool, gocql.UUID) {
	status, ok := m.byOrg[orgId]
	if !ok {
		m.byOrg[orgId] = metaRecordStatus{
			batchId:    newBatch,
			createdAt:  newCreatedAt,
			lastUpdate: newLastUpdate,
		}
		return true, newBatch
	}

	// if the current batch has been created at a time before the new batch,
	// then we want to make the new batch the current one and load its records
	if status.batchId != newBatch && status.createdAt < newCreatedAt {
		status.batchId = newBatch
		status.createdAt = newCreatedAt
		status.lastUpdate = newLastUpdate
		m.byOrg[orgId] = status
		return true, status.batchId
	}

	// if the current batch is the same as the new batch, but their last update times
	// differ, then we want to reload that batch
	if status.batchId == newBatch && status.lastUpdate != newLastUpdate {
		status.lastUpdate = newLastUpdate
		m.byOrg[orgId] = status
		return true, status.batchId
	}

	return false, defaultBatchId
}

func (m *metaRecordStatusByOrg) getStatus(orgId uint32) (gocql.UUID, uint64, uint64) {
	status, ok := m.byOrg[orgId]
	if !ok {
		return defaultBatchId, 0, 0
	}

	return status.batchId, status.createdAt, status.lastUpdate
}

func (c *CasIdx) initMetaRecords(session *gocql.Session) error {
	if !memory.MetaTagSupport || !memory.TagSupport {
		return nil
	}

	c.metaRecords = newMetaRecordStatusByOrg()

	err := c.EnsureTableExists(session, c.Config.SchemaFile, "schema_meta_record_table", c.Config.MetaRecordTable)
	if err != nil {
		return err
	}
	return c.EnsureTableExists(session, c.Config.SchemaFile, "schema_meta_record_batch_table", c.Config.MetaRecordBatchTable)
}

func (c *CasIdx) pollStore() {
	for {
		time.Sleep(c.Config.MetaRecordPollInterval)
		c.loadMetaRecords()
	}
}

func (c *CasIdx) loadMetaRecords() {
	q := fmt.Sprintf("SELECT batchid, orgid, createdat, lastupdate FROM %s", c.Config.MetaRecordBatchTable)
	iter := c.Session.Query(q).RetryPolicy(&metaRecordRetryPolicy).Iter()
	var batchId gocql.UUID
	var orgId uint32
	var createdAt, lastUpdate uint64
	toLoad := make(map[uint32]gocql.UUID)
	for iter.Scan(&batchId, &orgId, &createdAt, &lastUpdate) {
		load, batchId := c.metaRecords.update(orgId, batchId, createdAt, lastUpdate)
		if load {
			toLoad[orgId] = batchId
		}
	}
	var err error
	if err = iter.Close(); err != nil {
		log.Errorf("Error when loading batches of meta records: %s", err.Error())
		return
	}

	for orgId, batchId := range toLoad {
		var expressions, metatags string
		q = fmt.Sprintf("SELECT expressions, metatags FROM %s WHERE batchid=? AND orgid=?", c.Config.MetaRecordTable)
		iter = c.Session.Query(q, batchId, orgId).RetryPolicy(&metaRecordRetryPolicy).Iter()

		var records []tagquery.MetaTagRecord
		for iter.Scan(&expressions, &metatags) {
			record := tagquery.MetaTagRecord{}
			err = json.Unmarshal([]byte(expressions), &record.Expressions)
			if err != nil {
				log.Errorf("cassandra-idx: LoadMetaRecords() could not parse stored expressions (%s): %s", expressions, err)
				continue
			}

			err = json.Unmarshal([]byte(metatags), &record.MetaTags)
			if err != nil {
				log.Errorf("cassandra-idx: LoadMetaRecords() could not parse stored metatags (%s): %s", metatags, err)
				continue
			}

			records = append(records, record)
		}

		if err = iter.Close(); err != nil {
			log.Errorf("Error when reading meta records: %s", err.Error())
			continue
		}

		if err = c.MemoryIndex.MetaTagRecordSwap(orgId, records); err != nil {
			log.Errorf("Error when swapping batch of meta records: %s", err.Error())
			continue
		}
	}
}

func (c *CasIdx) MetaTagRecordUpsert(orgId uint32, record tagquery.MetaTagRecord) error {
	if !memory.MetaTagSupport || !memory.TagSupport {
		return errMetaTagSupportDisabled
	}
	if !c.Config.updateCassIdx {
		return errIdxUpdatesDisabled
	}

	var err error

	batchId, _, _ := c.metaRecords.getStatus(orgId)

	// if a record has no meta tags associated with it, then we delete it
	if len(record.MetaTags) > 0 {
		err = c.persistMetaRecord(orgId, batchId, record)
	} else {
		err = c.deleteMetaRecord(orgId, batchId, record)
	}

	if err != nil {
		log.Errorf("Failed to update meta records in cassandra: %s", err)
		return fmt.Errorf("Failed to update cassandra: %s", err)
	}

	err = c.markMetaRecordBatchUpdated(orgId, batchId)
	if err != nil {
		log.Errorf("Failed to update meta records in cassandra: %s", err)
		return fmt.Errorf("Failed to update cassandra: %s", err)
	}

	return nil
}

func (c *CasIdx) markMetaRecordBatchUpdated(orgId uint32, batchId gocql.UUID) error {
	now := time.Now().UnixNano() / 1000000
	if batchId == defaultBatchId {
		qry := fmt.Sprintf("INSERT INTO %s (orgid, batchid, createdat, lastupdate) VALUES (?, ?, ?, ?)", c.Config.MetaRecordBatchTable)
		return c.Session.Query(
			qry,
			orgId,
			batchId,
			0,
			now,
		).RetryPolicy(&metaRecordRetryPolicy).Exec()
	}

	qry := fmt.Sprintf("INSERT INTO %s (orgid, batchid, lastupdate) VALUES (?, ?, ?)", c.Config.MetaRecordBatchTable)
	return c.Session.Query(
		qry,
		orgId,
		batchId,
		now,
	).RetryPolicy(&metaRecordRetryPolicy).Exec()
}

func (c *CasIdx) MetaTagRecordSwap(orgId uint32, records []tagquery.MetaTagRecord) error {
	if !memory.MetaTagSupport || !memory.TagSupport {
		return errMetaTagSupportDisabled
	}
	if !c.Config.updateCassIdx {
		return errIdxUpdatesDisabled
	}

	newBatchId, err := gocql.RandomUUID()
	if err != nil {
		return fmt.Errorf("Failed to generate new batch id")
	}

	var expressions, metaTags []byte
	var qry string
	for _, record := range records {
		record.Expressions.Sort()
		expressions, err = record.Expressions.MarshalJSON()
		if err != nil {
			return fmt.Errorf("Failed to marshal expressions: %s", err)
		}
		metaTags, err = record.MetaTags.MarshalJSON()
		if err != nil {
			return fmt.Errorf("Failed to marshal meta tags: %s", err)
		}
		qry = fmt.Sprintf("INSERT INTO %s (batchid, orgid, expressions, metatags) VALUES (?, ?, ?, ?)", c.Config.MetaRecordTable)
		err = c.Session.Query(
			qry,
			newBatchId,
			orgId,
			expressions,
			metaTags,
		).RetryPolicy(&metaRecordRetryPolicy).Exec()
		if err != nil {
			return fmt.Errorf("Failed to save meta record: %s", err)
		}
	}

	return c.createNewBatch(orgId, newBatchId)
}

func (c *CasIdx) createNewBatch(orgId uint32, batchId gocql.UUID) error {
	now := time.Now().UnixNano() / 1000000
	qry := fmt.Sprintf("INSERT INTO %s (orgid, batchid, createdat, lastupdate) VALUES (?, ?, ?, ?)", c.Config.MetaRecordBatchTable)
	return c.Session.Query(
		qry,
		orgId,
		batchId,
		now,
		now,
	).RetryPolicy(&metaRecordRetryPolicy).Exec()
}

func (c *CasIdx) persistMetaRecord(orgId uint32, batchId gocql.UUID, record tagquery.MetaTagRecord) error {
	expressions, err := record.Expressions.MarshalJSON()
	if err != nil {
		return fmt.Errorf("Failed to marshal expressions: %s", err)
	}
	metaTags, err := record.MetaTags.MarshalJSON()
	if err != nil {
		return fmt.Errorf("Failed to marshal meta tags: %s", err)
	}

	qry := fmt.Sprintf("INSERT INTO %s (batchid, orgid, expressions, metatags) VALUES (?, ?, ?, ?)", c.Config.MetaRecordTable)
	return c.Session.Query(
		qry,
		batchId,
		orgId,
		expressions,
		metaTags).RetryPolicy(&metaRecordRetryPolicy).Exec()
}

func (c *CasIdx) deleteMetaRecord(orgId uint32, batchId gocql.UUID, record tagquery.MetaTagRecord) error {
	expressions, err := record.Expressions.MarshalJSON()
	if err != nil {
		return fmt.Errorf("Failed to marshal record expressions: %s", err)
	}

	qry := fmt.Sprintf("DELETE FROM %s WHERE batchid=? AND orgid=? AND expressions=?", c.Config.MetaRecordTable)
	return c.Session.Query(
		qry,
		batchId,
		orgId,
		expressions,
	).RetryPolicy(&metaRecordRetryPolicy).Exec()
}
