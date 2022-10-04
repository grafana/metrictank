package cassandra

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	cassUtils "github.com/grafana/metrictank/cassandra"
	"github.com/grafana/metrictank/expr/tagquery"
	"github.com/grafana/metrictank/idx"
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

type MetaRecordIdx struct {
	wg        sync.WaitGroup
	shutdown  chan struct{}
	cfg       *Config
	status    metatags.MetaRecordStatusByOrg
	memoryIdx idx.MetaRecordIdx
	cluster   *gocql.ClusterConfig
	session   *cassUtils.Session
}

func NewCassandraMetaRecordIdx(cfg *Config, memoryIdx idx.MetaRecordIdx) *MetaRecordIdx {
	if err := cfg.Validate(); err != nil {
		log.Fatalf("cass-meta-record-idx: %s", err)
	}
	cluster := gocql.NewCluster(strings.Split(cfg.hosts, ",")...)
	cluster.Consistency = gocql.ParseConsistency(cfg.consistency)
	cluster.Keyspace = cfg.keyspace
	cluster.Timeout = cfg.timeout
	cluster.ConnectTimeout = cluster.Timeout
	cluster.NumConns = cfg.numConns
	cluster.ProtoVersion = cfg.protoVer
	cluster.DisableInitialHostLookup = cfg.disableInitialHostLookup
	if cfg.ssl {
		cluster.SslOpts = &gocql.SslOptions{
			CaPath:                 cfg.caPath,
			EnableHostVerification: cfg.hostVerification,
		}
	}
	if cfg.auth {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: cfg.username,
			Password: cfg.password,
		}
	}

	return &MetaRecordIdx{
		shutdown:  make(chan struct{}),
		cfg:       cfg,
		status:    metatags.NewMetaRecordStatusByOrg(),
		memoryIdx: memoryIdx,
		cluster:   cluster,
	}
}

func (m *MetaRecordIdx) Init() error {
	var err error
	m.session, err = cassUtils.NewSession(m.cluster, m.cfg.connectionCheckTimeout, m.cfg.connectionCheckInterval, m.cfg.hosts, "cass-meta-record-idx")
	if err != nil {
		return fmt.Errorf("cass-meta-record-idx: Failed to create cassandra session: %s", err)
	}

	schema := fmt.Sprintf(util.ReadEntry(m.cfg.schemaFile, "schema_meta_record_table").(string), m.cfg.keyspace, m.cfg.metaRecordTable)
	err = cassUtils.EnsureTableExists(m.session.CurrentSession(), m.cfg.createKeyspace, m.cfg.keyspace, schema, m.cfg.metaRecordTable)
	if err != nil {
		return err
	}
	schema = fmt.Sprintf(util.ReadEntry(m.cfg.schemaFile, "schema_meta_record_batch_table").(string), m.cfg.keyspace, m.cfg.metaRecordBatchTable)
	err = cassUtils.EnsureTableExists(m.session.CurrentSession(), m.cfg.createKeyspace, m.cfg.keyspace, schema, m.cfg.metaRecordBatchTable)
	if err != nil {
		return err
	}

	m.loadMetaRecords()

	return nil
}

func (m *MetaRecordIdx) Start() {
	m.wg.Add(1)
	go m.pollStore()
	if m.cfg.updateCassIdx {
		m.wg.Add(1)
		go m.pruneMetaRecords()
	}
}

func (m *MetaRecordIdx) Stop() {
	close(m.shutdown)
	m.wg.Wait()
	m.session.Stop()
}

func (m *MetaRecordIdx) pollStore() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.cfg.metaRecordPollInterval)
	for {
		select {
		case <-m.shutdown:
			ticker.Stop()
			return
		case <-ticker.C:
			m.loadMetaRecords()
		}
	}
}

func (m *MetaRecordIdx) loadMetaRecords() {
	q := fmt.Sprintf("SELECT batchid, orgid, createdat, lastupdate FROM %s", m.cfg.metaRecordBatchTable)

	session := m.session.CurrentSession()
	iter := session.Query(q).RetryPolicy(&metaRecordRetryPolicy).Iter()
	var batchId gocql.UUID
	var orgId uint32
	var createdAt, lastUpdate uint64
	toLoad := make(map[uint32]gocql.UUID)
	for iter.Scan(&batchId, &orgId, &createdAt, &lastUpdate) {
		load, batchId := m.status.Update(orgId, metatags.UUID(batchId), createdAt, lastUpdate)
		if load {
			toLoad[orgId] = gocql.UUID(batchId)
		}
	}

	var err error
	if err = iter.Close(); err != nil {
		log.Errorf("cass-meta-record-idx: Error when loading batches of meta records: %s", err.Error())
		return
	}

	for orgId, batchId := range toLoad {
		log.Infof("cass-meta-record-idx: Loading meta record batch %s of org %d", batchId.String(), orgId)
		var expressions, metatags string
		q = fmt.Sprintf("SELECT expressions, metatags FROM %s WHERE batchid=? AND orgid=?", m.cfg.metaRecordTable)

		session := m.session.CurrentSession()
		iter = session.Query(q, batchId, orgId).RetryPolicy(&metaRecordRetryPolicy).Iter()

		var records []tagquery.MetaTagRecord
		for iter.Scan(&expressions, &metatags) {
			record := tagquery.MetaTagRecord{}
			err = json.Unmarshal([]byte(expressions), &record.Expressions)
			if err != nil {
				log.Errorf("cass-meta-record-idx: LoadMetaRecords() could not parse stored expressions (%s): %s", expressions, err)
				continue
			}

			err = json.Unmarshal([]byte(metatags), &record.MetaTags)
			if err != nil {
				log.Errorf("cass-meta-record-idx: LoadMetaRecords() could not parse stored metatags (%s): %s", metatags, err)
				continue
			}

			records = append(records, record)
		}

		if err = iter.Close(); err != nil {
			log.Errorf("cass-meta-record-idx: Error when reading meta records: %s", err.Error())
			continue
		}

		if err = m.memoryIdx.MetaTagRecordSwap(orgId, records); err != nil {
			log.Errorf("cass-meta-record-idx: Error when swapping batch of meta records: %s", err.Error())
			continue
		}
	}
}

func (m *MetaRecordIdx) pruneMetaRecords() {
	defer m.wg.Done()

	ticker := time.NewTicker(m.cfg.metaRecordPruneInterval)
	for {
		select {
		case <-m.shutdown:
			ticker.Stop()
			return
		case <-ticker.C:
			q := fmt.Sprintf("SELECT batchid, orgid, createdat FROM %s", m.cfg.metaRecordBatchTable)

			session := m.session.CurrentSession()
			iter := session.Query(q).RetryPolicy(&metaRecordRetryPolicy).Iter()
			var batchId gocql.UUID
			var orgId uint32
			var createdAt uint64
			for iter.Scan(&batchId, &orgId, &createdAt) {
				now := time.Now().Unix()
				if uint64(now)-uint64(m.cfg.metaRecordPruneAge.Seconds()) <= createdAt/1000 {
					continue
				}

				currentBatchId, _, _ := m.status.GetStatus(orgId)
				if batchId != gocql.UUID(currentBatchId) {
					err := m.pruneBatch(orgId, batchId)
					if err != nil {
						log.Errorf("cass-meta-record-idx: Error when pruning batch %d/%s: %s", orgId, batchId.String(), err)
					}
				}
			}
		}
	}
}

func (m *MetaRecordIdx) pruneBatch(orgId uint32, batchId gocql.UUID) error {
	qry := fmt.Sprintf("DELETE FROM %s WHERE orgid=? AND batchid=?", m.cfg.metaRecordTable)

	session := m.session.CurrentSession()
	err := session.Query(
		qry,
		orgId,
		batchId,
	).RetryPolicy(&metaRecordRetryPolicy).Exec()
	if err != nil {
		return err
	}

	qry = fmt.Sprintf("DELETE FROM %s WHERE orgid=? AND batchid=?", m.cfg.metaRecordBatchTable)
	return session.Query(
		qry,
		orgId,
		batchId,
	).RetryPolicy(&metaRecordRetryPolicy).Exec()
}

func (m *MetaRecordIdx) MetaTagRecordList(orgId uint32) []tagquery.MetaTagRecord {
	return m.memoryIdx.MetaTagRecordList(orgId)
}

func (m *MetaRecordIdx) MetaTagRecordUpsert(orgId uint32, record tagquery.MetaTagRecord) error {
	if !m.cfg.updateCassIdx {
		return errIdxUpdatesDisabled
	}

	var err error

	batchId, _, _ := m.status.GetStatus(orgId)

	// if a record has no meta tags associated with it, then we delete it
	if len(record.MetaTags) > 0 {
		err = m.persistMetaRecord(orgId, batchId, record)
	} else {
		err = m.deleteMetaRecord(orgId, batchId, record)
	}

	if err != nil {
		log.Errorf("cass-meta-record-idx: Failed to update meta records in cassandra: %s", err)
		return fmt.Errorf("Failed to update cassandra: %s", err)
	}

	err = m.markMetaRecordBatchUpdated(orgId, batchId)
	if err != nil {
		log.Errorf("cass-meta-record-idx: Failed to update meta records in cassandra: %s", err)
		return fmt.Errorf("Failed to update cassandra: %s", err)
	}

	return nil
}

func (m *MetaRecordIdx) markMetaRecordBatchUpdated(orgId uint32, batchId metatags.UUID) error {
	session := m.session.CurrentSession()
	now := time.Now().UnixNano() / 1000000
	if batchId == metatags.DefaultBatchId {
		qry := fmt.Sprintf("INSERT INTO %s (orgid, batchid, createdat, lastupdate) VALUES (?, ?, ?, ?)", m.cfg.metaRecordBatchTable)
		return session.Query(
			qry,
			orgId,
			gocql.UUID(batchId),
			0,
			now,
		).RetryPolicy(&metaRecordRetryPolicy).Exec()
	}

	qry := fmt.Sprintf("INSERT INTO %s (orgid, batchid, lastupdate) VALUES (?, ?, ?)", m.cfg.metaRecordBatchTable)
	return session.Query(
		qry,
		orgId,
		gocql.UUID(batchId),
		now,
	).RetryPolicy(&metaRecordRetryPolicy).Exec()
}

func (m *MetaRecordIdx) MetaTagRecordSwap(orgId uint32, records []tagquery.MetaTagRecord) error {
	if !m.cfg.updateCassIdx {
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
		qry = fmt.Sprintf("INSERT INTO %s (batchid, orgid, expressions, metatags) VALUES (?, ?, ?, ?)", m.cfg.metaRecordTable)

		session := m.session.CurrentSession()
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

	return m.createNewBatch(orgId, newBatchId)
}

func (m *MetaRecordIdx) createNewBatch(orgId uint32, batchId metatags.UUID) error {
	session := m.session.CurrentSession()
	now := time.Now().UnixNano() / 1000000
	qry := fmt.Sprintf("INSERT INTO %s (orgid, batchid, createdat, lastupdate) VALUES (?, ?, ?, ?)", m.cfg.metaRecordBatchTable)
	return session.Query(
		qry,
		orgId,
		gocql.UUID(batchId),
		now,
		now,
	).RetryPolicy(&metaRecordRetryPolicy).Exec()
}

func (m *MetaRecordIdx) persistMetaRecord(orgId uint32, batchId metatags.UUID, record tagquery.MetaTagRecord) error {
	expressions, err := record.Expressions.MarshalJSON()
	if err != nil {
		return fmt.Errorf("Failed to marshal expressions: %s", err)
	}
	metaTags, err := record.MetaTags.MarshalJSON()
	if err != nil {
		return fmt.Errorf("Failed to marshal meta tags: %s", err)
	}

	session := m.session.CurrentSession()
	qry := fmt.Sprintf("INSERT INTO %s (batchid, orgid, expressions, metatags) VALUES (?, ?, ?, ?)", m.cfg.metaRecordTable)
	return session.Query(
		qry,
		gocql.UUID(batchId),
		orgId,
		expressions,
		metaTags).RetryPolicy(&metaRecordRetryPolicy).Exec()
}

func (m *MetaRecordIdx) deleteMetaRecord(orgId uint32, batchId metatags.UUID, record tagquery.MetaTagRecord) error {
	expressions, err := record.Expressions.MarshalJSON()
	if err != nil {
		return fmt.Errorf("Failed to marshal record expressions: %s", err)
	}

	session := m.session.CurrentSession()
	qry := fmt.Sprintf("DELETE FROM %s WHERE batchid=? AND orgid=? AND expressions=?", m.cfg.metaRecordTable)
	return session.Query(
		qry,
		gocql.UUID(batchId),
		orgId,
		expressions,
	).RetryPolicy(&metaRecordRetryPolicy).Exec()
}
