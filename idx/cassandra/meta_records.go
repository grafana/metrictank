package cassandra

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	cassUtils "github.com/grafana/metrictank/cassandra"
	"github.com/grafana/metrictank/expr/tagquery"
	"github.com/grafana/metrictank/idx/memory"
	"github.com/grafana/metrictank/idx/metatags"
	"github.com/grafana/metrictank/util"
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

	errIdxUpdatesDisabled = fmt.Errorf("Cassandra index updates are disabled")
)

func (c *CasIdx) initMetaRecords(session *gocql.Session) error {
	if !memory.MetaTagSupport || !memory.TagSupport {
		return nil
	}

	c.metaRecords = metatags.NewMetaRecordStatusByOrg()

	schema := fmt.Sprintf(util.ReadEntry(c.Config.SchemaFile, "schema_meta_record_table").(string), c.Config.Keyspace, c.Config.MetaRecordTable)
	err := cassUtils.EnsureTableExists(session, c.Config.CreateKeyspace, c.Config.Keyspace, schema, c.Config.MetaRecordTable)
	if err != nil {
		return err
	}
	schema = fmt.Sprintf(util.ReadEntry(c.Config.SchemaFile, "schema_meta_record_batch_table").(string), c.Config.Keyspace, c.Config.MetaRecordBatchTable)
	return cassUtils.EnsureTableExists(session, c.Config.CreateKeyspace, c.Config.Keyspace, schema, c.Config.MetaRecordBatchTable)
}

func (c *CasIdx) pollStore() {
	defer c.wg.Done()
	for {
		select {
		case <-c.shutdown:
			return
		default:
			c.loadMetaRecords()
			time.Sleep(c.Config.MetaRecordPollInterval)
		}
	}
}

func (c *CasIdx) loadMetaRecords() {
	q := fmt.Sprintf("SELECT batchid, orgid, createdat, lastupdate FROM %s", c.Config.MetaRecordBatchTable)

	session := c.Session.CurrentSession()
	iter := session.Query(q).RetryPolicy(&metaRecordRetryPolicy).Iter()
	var batchId gocql.UUID
	var orgId uint32
	var createdAt, lastUpdate uint64
	toLoad := make(map[uint32]gocql.UUID)
	for iter.Scan(&batchId, &orgId, &createdAt, &lastUpdate) {
		load, batchId := c.metaRecords.Update(orgId, metatags.UUID(batchId), createdAt, lastUpdate)
		if load {
			toLoad[orgId] = gocql.UUID(batchId)
		}
	}

	var err error
	if err = iter.Close(); err != nil {
		log.Errorf("Error when loading batches of meta records: %s", err.Error())
		return
	}

	for orgId, batchId := range toLoad {
		log.Infof("cassandra-idx: Loading meta record batch %s of org %d", batchId.String(), orgId)
		var expressions, metatags string
		q = fmt.Sprintf("SELECT expressions, metatags FROM %s WHERE batchid=? AND orgid=?", c.Config.MetaRecordTable)

		session := c.Session.CurrentSession()
		iter = session.Query(q, batchId, orgId).RetryPolicy(&metaRecordRetryPolicy).Iter()

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

func (c *CasIdx) pruneMetaRecords() {
	defer c.wg.Done()
	for {
		select {
		case <-c.shutdown:
			return
		default:
			q := fmt.Sprintf("SELECT batchid, orgid, createdat FROM %s", c.Config.MetaRecordBatchTable)

			session := c.Session.CurrentSession()
			iter := session.Query(q).RetryPolicy(&metaRecordRetryPolicy).Iter()
			var batchId gocql.UUID
			var orgId uint32
			var createdAt uint64
			for iter.Scan(&batchId, &orgId, &createdAt) {
				now := time.Now().Unix()
				if uint64(now)-uint64(c.Config.MetaRecordPruneAge.Seconds()) <= createdAt/1000 {
					continue
				}

				currentBatchId, _, _ := c.metaRecords.GetStatus(orgId)
				if batchId != gocql.UUID(currentBatchId) {
					err := c.pruneBatch(orgId, batchId)
					if err != nil {
						log.Errorf("Error when pruning batch %d/%s: %s", orgId, batchId.String(), err)
					}
				}
			}
			time.Sleep(c.Config.MetaRecordPruneInterval)
		}
	}
}

func (c *CasIdx) pruneBatch(orgId uint32, batchId gocql.UUID) error {
	qry := fmt.Sprintf("DELETE FROM %s WHERE orgid=? AND batchid=?", c.Config.MetaRecordTable)

	session := c.Session.CurrentSession()
	err := session.Query(
		qry,
		orgId,
		batchId,
	).RetryPolicy(&metaRecordRetryPolicy).Exec()
	if err != nil {
		return err
	}

	qry = fmt.Sprintf("DELETE FROM %s WHERE orgid=? AND batchid=?", c.Config.MetaRecordBatchTable)
	return session.Query(
		qry,
		orgId,
		batchId,
	).RetryPolicy(&metaRecordRetryPolicy).Exec()
}

func (c *CasIdx) MetaTagRecordUpsert(orgId uint32, record tagquery.MetaTagRecord) error {
	if !memory.MetaTagSupport || !memory.TagSupport {
		return metatags.ErrMetaTagSupportDisabled
	}
	if !c.Config.updateCassIdx {
		return errIdxUpdatesDisabled
	}

	var err error

	batchId, _, _ := c.metaRecords.GetStatus(orgId)

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

func (c *CasIdx) markMetaRecordBatchUpdated(orgId uint32, batchId metatags.UUID) error {
	session := c.Session.CurrentSession()
	now := time.Now().UnixNano() / 1000000
	if batchId == metatags.DefaultBatchId {
		qry := fmt.Sprintf("INSERT INTO %s (orgid, batchid, createdat, lastupdate) VALUES (?, ?, ?, ?)", c.Config.MetaRecordBatchTable)
		return session.Query(
			qry,
			orgId,
			gocql.UUID(batchId),
			0,
			now,
		).RetryPolicy(&metaRecordRetryPolicy).Exec()
	}

	qry := fmt.Sprintf("INSERT INTO %s (orgid, batchid, lastupdate) VALUES (?, ?, ?)", c.Config.MetaRecordBatchTable)
	return session.Query(
		qry,
		orgId,
		gocql.UUID(batchId),
		now,
	).RetryPolicy(&metaRecordRetryPolicy).Exec()
}

func (c *CasIdx) MetaTagRecordSwap(orgId uint32, records []tagquery.MetaTagRecord) error {
	if !memory.MetaTagSupport || !memory.TagSupport {
		return metatags.ErrMetaTagSupportDisabled
	}
	if !c.Config.updateCassIdx {
		return errIdxUpdatesDisabled
	}

	newBatchId, err := metatags.RandomUUID()
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

		session := c.Session.CurrentSession()
		err = session.Query(
			qry,
			gocql.UUID(newBatchId),
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

func (c *CasIdx) createNewBatch(orgId uint32, batchId metatags.UUID) error {
	session := c.Session.CurrentSession()
	now := time.Now().UnixNano() / 1000000
	qry := fmt.Sprintf("INSERT INTO %s (orgid, batchid, createdat, lastupdate) VALUES (?, ?, ?, ?)", c.Config.MetaRecordBatchTable)
	return session.Query(
		qry,
		orgId,
		gocql.UUID(batchId),
		now,
		now,
	).RetryPolicy(&metaRecordRetryPolicy).Exec()
}

func (c *CasIdx) persistMetaRecord(orgId uint32, batchId metatags.UUID, record tagquery.MetaTagRecord) error {
	expressions, err := record.Expressions.MarshalJSON()
	if err != nil {
		return fmt.Errorf("Failed to marshal expressions: %s", err)
	}
	metaTags, err := record.MetaTags.MarshalJSON()
	if err != nil {
		return fmt.Errorf("Failed to marshal meta tags: %s", err)
	}

	session := c.Session.CurrentSession()
	qry := fmt.Sprintf("INSERT INTO %s (batchid, orgid, expressions, metatags) VALUES (?, ?, ?, ?)", c.Config.MetaRecordTable)
	return session.Query(
		qry,
		gocql.UUID(batchId),
		orgId,
		expressions,
		metaTags).RetryPolicy(&metaRecordRetryPolicy).Exec()
}

func (c *CasIdx) deleteMetaRecord(orgId uint32, batchId metatags.UUID, record tagquery.MetaTagRecord) error {
	expressions, err := record.Expressions.MarshalJSON()
	if err != nil {
		return fmt.Errorf("Failed to marshal record expressions: %s", err)
	}

	session := c.Session.CurrentSession()
	qry := fmt.Sprintf("DELETE FROM %s WHERE batchid=? AND orgid=? AND expressions=?", c.Config.MetaRecordTable)
	return session.Query(
		qry,
		gocql.UUID(batchId),
		orgId,
		expressions,
	).RetryPolicy(&metaRecordRetryPolicy).Exec()
}
