package cassandra

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/grafana/metrictank/schema"

	"github.com/gocql/gocql"
	"github.com/grafana/metrictank/cassandra"
	cassUtils "github.com/grafana/metrictank/cassandra"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/mdata/chunk"
	"github.com/grafana/metrictank/stats"
	"github.com/grafana/metrictank/util"
	hostpool "github.com/hailocab/go-hostpool"
	opentracing "github.com/opentracing/opentracing-go"
	log "github.com/sirupsen/logrus"
)

// write aggregated data to cassandra.

const Month_sec = 60 * 60 * 24 * 28

var (
	errChunkTooSmall = errors.New("impossibly small chunk in cassandra")
	errInvalidRange  = errors.New("CassandraStore: invalid range: from must be less than to")
	errCtxCanceled   = errors.New("context canceled")
	errReadQueueFull = errors.New("the read queue is full")
	errReadTooOld    = errors.New("the read is too old")
	errTableNotFound = errors.New("table for given TTL not found")

	// metric store.cassandra.get.exec is the duration of getting from cassandra store
	cassGetExecDuration = stats.NewLatencyHistogram15s32("store.cassandra.get.exec")
	// metric store.cassandra.get.wait is the duration of the get spent in the queue
	cassGetWaitDuration = stats.NewLatencyHistogram12h32("store.cassandra.get.wait")
	// metric store.cassandra.put.exec is the duration of putting in cassandra store
	cassPutExecDuration = stats.NewLatencyHistogram15s32("store.cassandra.put.exec")
	// metric store.cassandra.put.wait is the duration of a put in the wait queue
	cassPutWaitDuration = stats.NewLatencyHistogram12h32("store.cassandra.put.wait")
	// reads that were already too old to be executed
	cassOmitOldRead = stats.NewCounter32("store.cassandra.omit_read.too_old")
	// reads that could not be pushed into the queue because it was full
	cassReadQueueFull = stats.NewCounter32("store.cassandra.omit_read.queue_full")

	// metric store.cassandra.chunks_per_response is how many chunks are retrieved per response in get queries
	cassChunksPerResponse = stats.NewMeter32("store.cassandra.chunks_per_response", false)
	// metric store.cassandra.rows_per_response is how many rows come per get response
	cassRowsPerResponse = stats.NewMeter32("store.cassandra.rows_per_response", false)
	// metric store.cassandra.get_chunks is the duration of how long it takes to get chunks
	cassGetChunksDuration = stats.NewLatencyHistogram15s32("store.cassandra.get_chunks")
	// metric store.cassandra.to_iter is the duration of converting chunks to iterators
	cassToIterDuration = stats.NewLatencyHistogram15s32("store.cassandra.to_iter")

	// metric store.cassandra.chunk_operations.save_ok is counter of successful saves
	chunkSaveOk = stats.NewCounter32("store.cassandra.chunk_operations.save_ok")
	// metric store.cassandra.chunk_operations.save_fail is counter of failed saves
	chunkSaveFail = stats.NewCounter32("store.cassandra.chunk_operations.save_fail")
	// metric store.cassandra.chunk_size.at_save is the sizes of chunks seen when saving them
	chunkSizeAtSave = stats.NewMeter32("store.cassandra.chunk_size.at_save", true)
	// metric store.cassandra.chunk_size.at_load is the sizes of chunks seen when loading them
	chunkSizeAtLoad = stats.NewMeter32("store.cassandra.chunk_size.at_load", true)

	errmetrics = cassandra.NewErrMetrics("store.cassandra")
)

type ChunkReadRequest struct {
	q         string
	p         []interface{}
	timestamp time.Time
	out       chan readResult
	ctx       context.Context
}

type CassandraStore struct {
	Session          *cassandra.Session
	cluster          *gocql.ClusterConfig
	writeQueues      []chan *mdata.ChunkWriteRequest
	writeQueueMeters []*stats.Range32
	readQueue        chan *ChunkReadRequest
	TTLTables        TTLTables
	omitReadTimeout  time.Duration
	tracer           opentracing.Tracer
	shutdown         chan struct{}
	wg               sync.WaitGroup
}

// ConvertTimeout provides backwards compatibility for values that used to be specified as integers,
// while also allowing them to be specified as durations.
func ConvertTimeout(timeout string, defaultUnit time.Duration) time.Duration {
	if timeoutI, err := strconv.Atoi(timeout); err == nil {
		log.Warn("cassandra-store: specifying the timeout as integer is deprecated, please use a duration value")
		return time.Duration(timeoutI) * defaultUnit
	}
	timeoutD, err := time.ParseDuration(timeout)
	if err != nil {
		log.Fatalf("cassandra-store: invalid duration value %q", timeout)
	}
	return timeoutD
}

// NewCassandraStore creates a new cassandra store, using the provided retention ttl's in seconds
func NewCassandraStore(config *StoreConfig, ttls []uint32) (*CassandraStore, error) {
	stats.NewGauge32("store.cassandra.write_queue.size").Set(config.WriteQueueSize)
	stats.NewGauge32("store.cassandra.num_writers").Set(config.WriteConcurrency)

	cluster := gocql.NewCluster(strings.Split(config.Addrs, ",")...)
	if config.SSL {
		cluster.SslOpts = &gocql.SslOptions{
			CaPath:                 config.CaPath,
			EnableHostVerification: config.HostVerification,
		}
	}
	if config.Auth {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: config.Username,
			Password: config.Password,
		}
	}

	cluster.Timeout = ConvertTimeout(config.Timeout, time.Millisecond)
	cluster.Consistency = gocql.ParseConsistency(config.Consistency)
	cluster.ConnectTimeout = cluster.Timeout
	cluster.NumConns = config.WriteConcurrency
	cluster.ProtoVersion = config.CqlProtocolVersion
	cluster.DisableInitialHostLookup = config.DisableInitialHostLookup
	var err error
	tmpSession, err := cluster.CreateSession()
	if err != nil {
		log.Errorf("cassandra-store: failed to create cassandra session. %s", err.Error())
		return nil, err
	}

	schemaKeyspace := util.ReadEntry(config.SchemaFile, "schema_keyspace").(string)
	schemaTable := util.ReadEntry(config.SchemaFile, "schema_table").(string)

	ttlTables := GetTTLTables(ttls, config.WindowFactor, Table_name_format)

	// create or verify the metrictank keyspace
	if config.CreateKeyspace {
		log.Infof("cassandra-store: ensuring that keyspace %s exists.", config.Keyspace)
		err = tmpSession.Query(fmt.Sprintf(schemaKeyspace, config.Keyspace)).Exec()
		if err != nil {
			return nil, err
		}
	}

	for _, table := range ttlTables {
		log.Infof("cassandra-store: ensuring that table %s exists.", table.Name)

		schema := fmt.Sprintf(schemaTable, config.Keyspace, table.Name, table.WindowSize, table.WindowSize*60*60)
		err := cassUtils.EnsureTableExists(tmpSession, config.CreateKeyspace, config.Keyspace, schema, table.Name)
		if err != nil {
			return nil, err
		}
	}

	tmpSession.Close()
	cluster.Keyspace = config.Keyspace
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: config.Retries}

	switch config.HostSelectionPolicy {
	case "roundrobin":
		cluster.PoolConfig.HostSelectionPolicy = gocql.RoundRobinHostPolicy()
	case "hostpool-simple":
		cluster.PoolConfig.HostSelectionPolicy = gocql.HostPoolHostPolicy(hostpool.New(nil))
	case "hostpool-epsilon-greedy":
		cluster.PoolConfig.HostSelectionPolicy = gocql.HostPoolHostPolicy(
			hostpool.NewEpsilonGreedy(nil, 0, &hostpool.LinearEpsilonValueCalculator{}),
		)
	case "tokenaware,roundrobin":
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(
			gocql.RoundRobinHostPolicy(),
		)
	case "tokenaware,hostpool-simple":
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(
			gocql.HostPoolHostPolicy(hostpool.New(nil)),
		)
	case "tokenaware,hostpool-epsilon-greedy":
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(
			gocql.HostPoolHostPolicy(
				hostpool.NewEpsilonGreedy(nil, 0, &hostpool.LinearEpsilonValueCalculator{}),
			),
		)
	default:
		return nil, fmt.Errorf("unknown HostSelectionPolicy '%q'", config.HostSelectionPolicy)
	}

	cs, err := cassandra.NewSession(cluster, config.ConnectionCheckTimeout, config.ConnectionCheckInterval, config.Addrs, "cassandra-store")

	if err != nil {
		return nil, err
	}

	log.Debugf("cassandra-store: created session with config %+v", config)
	c := &CassandraStore{
		Session:          cs,
		cluster:          cluster,
		writeQueues:      make([]chan *mdata.ChunkWriteRequest, config.WriteConcurrency),
		writeQueueMeters: make([]*stats.Range32, config.WriteConcurrency),
		readQueue:        make(chan *ChunkReadRequest, config.ReadQueueSize),
		omitReadTimeout:  ConvertTimeout(config.OmitReadTimeout, time.Second),
		TTLTables:        ttlTables,
		tracer:           opentracing.NoopTracer{},
		shutdown:         make(chan struct{}),
	}

	for i := 0; i < config.WriteConcurrency; i++ {
		c.writeQueues[i] = make(chan *mdata.ChunkWriteRequest, config.WriteQueueSize)
		c.writeQueueMeters[i] = stats.NewRange32(fmt.Sprintf("store.cassandra.write_queue.%d.items", i+1))
		c.wg.Add(1)
		go c.processWriteQueue(c.writeQueues[i], c.writeQueueMeters[i])
	}

	for i := 0; i < config.ReadConcurrency; i++ {
		c.wg.Add(1)
		go c.processReadQueue()
	}

	return c, err
}

// FindExistingTables set's the store's table definitions to what it can find
// in the database.
// WARNING:
// * does not set the windowSize property, because we don't know what the windowFactor was
//   we could actually figure it based on the table definition, assuming the schema isn't tampered with,
//   but there is no use case for this so we haven't implemented this.
// * each table covers a range of TTL's. we set the TTL to the lower limit
//   so remember the TTL might have been up to twice as much
func (c *CassandraStore) FindExistingTables(keyspace string) error {

	session := c.Session.CurrentSession()
	meta, err := session.KeyspaceMetadata(keyspace)
	if err != nil {
		return err
	}

	c.TTLTables = make(TTLTables)

	for _, table := range meta.Tables {
		if !IsStoreTable(table.Name) {
			continue
		}
		fields := strings.Split(table.Name, "_")
		if len(fields) != 2 {
			return fmt.Errorf("could not parse table %q", table.Name)
		}

		ttl, err := strconv.Atoi(fields[1])
		if err != nil {
			return fmt.Errorf("could not parse table %q", table.Name)
		}
		c.TTLTables[uint32(ttl)] = Table{
			Name:       table.Name,
			QueryRead:  fmt.Sprintf(QueryFmtRead, table.Name),
			QueryWrite: fmt.Sprintf(QueryFmtWrite, table.Name),
			TTL:        uint32(ttl),
		}
	}
	return nil
}

func (c *CassandraStore) SetTracer(t opentracing.Tracer) {
	c.tracer = t
}

func (c *CassandraStore) Add(cwr *mdata.ChunkWriteRequest) {
	sum := int(cwr.Key.MKey.Org)
	for _, b := range cwr.Key.MKey.Key {
		sum += int(b)
	}
	which := sum % len(c.writeQueues)
	c.writeQueueMeters[which].Value(len(c.writeQueues[which]))
	c.writeQueues[which] <- cwr
}

/* process writeQueue.
 */
func (c *CassandraStore) processWriteQueue(queue chan *mdata.ChunkWriteRequest, meter *stats.Range32) {
	defer c.wg.Done()

	ticker := time.NewTicker(time.Second)

	for {
		select {
		case <-c.shutdown:
			log.Info("cassandra-store: received shutdown, exiting processWriteQueue")
			ticker.Stop()
			return
		case <-ticker.C:
			meter.Value(len(queue))
		case cwr := <-queue:
			meter.Value(len(queue))
			keyStr := cwr.Key.String()
			if log.IsLevelEnabled(log.DebugLevel) {
				log.Debugf("cassandra-store: starting to save %s:%d %v", keyStr, cwr.T0, cwr.Data)
			}
			//log how long the chunk waited in the queue before we attempted to save to cassandra
			cassPutWaitDuration.Value(time.Now().Sub(cwr.Timestamp))

			chunkSizeAtSave.Value(len(cwr.Data))
			success := false
			attempts := 0
			for !success {
				select {
				case <-c.shutdown:
					log.Info("cassandra-store: received shutdown, exiting processWriteQueue")
					ticker.Stop()
					return
				default:
					err := c.insertChunk(keyStr, cwr.T0, cwr.TTL, cwr.Data)

					if err == nil {
						success = true
						if cwr.Callback != nil {
							cwr.Callback()
						}
						if log.IsLevelEnabled(log.DebugLevel) {
							log.Debugf("cassandra-store: save complete. %s:%d %v", keyStr, cwr.T0, cwr.Data)
						}
						chunkSaveOk.Inc()
					} else {
						errmetrics.Inc(err)
						if (attempts % 20) == 0 {
							log.Warnf("cassandra-store: failed to save chunk to cassandra after %d attempts. %v, %s", attempts+1, cwr.Data, err)
						}
						chunkSaveFail.Inc()
						sleepTime := 100 * attempts
						if sleepTime > 2000 {
							sleepTime = 2000
						}
						time.Sleep(time.Duration(sleepTime) * time.Millisecond)
						attempts++
					}
				}
			}
		}
	}
}

// Insert Chunks into Cassandra.
//
// key: is the metric_id
// ts: is the start of the aggregated time range.
// data: is the payload as bytes.
func (c *CassandraStore) insertChunk(key string, t0, ttl uint32, data []byte) error {
	// for unit tests
	if c.Session == nil {
		return nil
	}

	span := chunk.ExtractChunkSpan(data)
	if span == 0 {
		span = mdata.MaxChunkSpan()
	}

	// we calculate ttl like this:
	// - the chunk's t0 plus its span is the ts of last possible datapoint in the chunk
	// - the timestamp of the last datapoint + ttl is the timestamp until when we want to keep this chunk
	// - then we subtract the current time stamp to get the difference relative to now
	// - the result is the ttl in seconds relative to now
	relativeTtl := int64(t0+span+ttl) - time.Now().Unix()

	// if the ttl relative to now is <=0 then we can omit the insert
	if relativeTtl <= 0 {
		if log.IsLevelEnabled(log.DebugLevel) {
			log.Debugf("Omitting insert of chunk with ttl %d: %s, %d", relativeTtl, key, t0)
		}
		return nil
	}

	table, ok := c.TTLTables[ttl]
	if !ok {
		return errTableNotFound
	}

	row_key := fmt.Sprintf("%s_%d", key, t0/Month_sec) // "month number" based on unix timestamp (rounded down)
	pre := time.Now()
	ctx, cancel := context.WithTimeout(context.Background(), c.cluster.Timeout)
	session := c.Session.CurrentSession()
	ret := session.Query(table.QueryWrite, row_key, t0, data, uint32(relativeTtl)).WithContext(ctx).Exec()
	cancel()
	cassPutExecDuration.Value(time.Now().Sub(pre))
	return ret
}

type readResult struct {
	i   *gocql.Iter
	err error
}

func (c *CassandraStore) processReadQueue() {
	defer c.wg.Done()

	for {
		select {
		case crr := <-c.readQueue:
			// check to see if the request has been canceled, if so abort now.
			select {
			case <-crr.ctx.Done():
				//request canceled
				crr.out <- readResult{err: errCtxCanceled}
				continue
			default:
			}
			waitDuration := time.Since(crr.timestamp)
			cassGetWaitDuration.Value(waitDuration)
			if waitDuration > c.omitReadTimeout {
				cassOmitOldRead.Inc()
				crr.out <- readResult{err: errReadTooOld}
				continue
			}

			pre := time.Now()
			session := c.Session.CurrentSession()
			iter := readResult{
				i:   session.Query(crr.q, crr.p...).WithContext(crr.ctx).Iter(),
				err: nil,
			}
			cassGetExecDuration.Value(time.Since(pre))
			crr.out <- iter
		case <-c.shutdown:
			log.Info("cassandra-store: received shutdown, exiting processReadQueue")
			return
		}
	}
}

// Basic search of cassandra in the table for given ttl
// start inclusive, end exclusive
func (c *CassandraStore) Search(ctx context.Context, key schema.AMKey, ttl, start, end uint32) ([]chunk.IterGen, error) {
	table, ok := c.TTLTables[ttl]
	if !ok {
		return nil, errTableNotFound
	}
	return c.SearchTable(ctx, key, table, start, end)
}

// Basic search of cassandra in given table
// start inclusive, end exclusive
func (c *CassandraStore) SearchTable(ctx context.Context, key schema.AMKey, table Table, start, end uint32) ([]chunk.IterGen, error) {
	var itgens []chunk.IterGen
	if start >= end {
		return itgens, errInvalidRange
	}

	pre := time.Now()

	// unfortunately in the database we only have the t0's of all chunks.
	// this means we can easily make sure to include the correct last chunk (just query for a t0 < end, the last chunk will contain the last needed data)
	// but it becomes hard to find which should be the first chunk to include. we can't just query for start <= t0 because then we will miss some data at
	// the beginning. We can't assume we know the chunkSpan so we can't just calculate the t0 >= (start - <some-predefined-number>) because chunkSpans
	// may change over time.
	// we effectively need all chunks with a t0 > start, as well as the last chunk with a t0 <= start.
	// since we make sure that you can only use chunkSpans so that Month_sec % chunkSpan == 0, we know that this previous chunk will always be in the same row
	// as startMonth

	// For example:
	// Month_sec = 60 * 60 * 24 * 28 = 2419200 (28 days)
	// Chunkspan = 60 * 60 * 24 * 7  = 604800  (7 days) this is much more than typical chunk size, but this allows us to be more compact in this example
	// row chunk t0      st. end
	// 0   0     2419200
	//     1     3024000
	//     2     3628800
	//     3     4233600
	// 1   4     4838400  /
	//     5     5443200  \
	//     6     6048000
	//     7     6652800
	// 2   8     7257600     /
	//     9     7862400     \
	//     ...   ...

	// let's say query has start 5222000 and end 7555000
	// so start is somewhere between 4-5, and end between 8-9
	// startMonth = 5222000 / 2419200 = 2 (row 1)
	// endMonth = 7554999 / 2419200 = 3 (row 2)
	// how do we query for all the chunks we need and not many more? knowing that chunkspan is not known?
	// for end, we can simply search for t0 < 7555000 in row 2, which gives us all chunks we need
	// for start, the best we can do is search for t0 <= 5222000 in row 1
	// note that this may include up to 4 weeks of unneeded data if start falls late within a month.  NOTE: we can set chunkspan "hints" via config

	results := make(chan readResult, 1)

	startMonth := start / Month_sec   // starting row has to be at, or before, requested start
	endMonth := (end - 1) / Month_sec // ending row has to include the last point we might need (end-1)
	rowKeys := make([]string, endMonth-startMonth+1)
	i := 0
	for num := startMonth; num <= endMonth; num++ {
		rowKeys[i] = fmt.Sprintf("%s_%d", key, num)
		i++
	}
	crr := ChunkReadRequest{
		q:         table.QueryRead,
		p:         []interface{}{rowKeys, end},
		timestamp: pre,
		out:       results,
		ctx:       ctx,
	}

	select {
	case <-ctx.Done():
		// request has been canceled, so no need to continue queuing reads.
		// reads already queued will be aborted when read from the queue.
		return nil, nil
	case c.readQueue <- &crr:
	default:
		cassReadQueueFull.Inc()
		return nil, errReadQueueFull
	}

	var res readResult
	select {
	case <-ctx.Done():
		// request has been canceled, so no need to continue processing results
		return nil, nil
	case res = <-results:
		if res.err != nil {
			if res.err == errCtxCanceled {
				// context was canceled, return immediately.
				return nil, nil
			}
			return nil, res.err
		}
		close(results)
	}

	cassGetChunksDuration.Value(time.Since(pre))
	pre = time.Now()

	intervalHint := key.Archive.Span()

	var b []byte
	var t0 int
	for res.i.Scan(&t0, &b) {
		chunkSizeAtLoad.Value(len(b))
		if len(b) < 2 {
			return itgens, errChunkTooSmall
		}
		// As 'b' is re-used for each scan, we need to make a copy of the []byte slice before assigning it to a new IterGen.
		safeBytes := make([]byte, len(b))
		copy(safeBytes, b)
		itgen, err := chunk.NewIterGen(uint32(t0), intervalHint, safeBytes)
		if err != nil {
			return itgens, err
		}
		itgens = append(itgens, itgen)
	}

	err := res.i.Close()
	if err != nil {
		if err == context.Canceled || err == context.DeadlineExceeded {
			// query was aborted.
			return nil, nil
		}
		errmetrics.Inc(err)
		return nil, err
	}

	sort.Sort(chunk.IterGensAsc(itgens))

	cassToIterDuration.Value(time.Now().Sub(pre))
	cassRowsPerResponse.Value(len(rowKeys))
	cassChunksPerResponse.Value(len(itgens))
	return itgens, nil
}

func (c *CassandraStore) Stop() {
	close(c.shutdown)
	c.wg.Wait()
	c.Session.Stop()
}
