package main

import (
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	hostpool "github.com/hailocab/go-hostpool"
)

const minToken = math.MinInt64
const maxToken = math.MaxInt64 // 9223372036854775807

var (
	sourceCassandraAddrs         = flag.String("source-cassandra-addrs", "localhost", "cassandra host (may be given multiple times as comma-separated list)")
	destCassandraAddrs           = flag.String("dest-cassandra-addrs", "localhost", "cassandra host (may be given multiple times as comma-separated list)")
	cassandraKeyspace            = flag.String("cassandra-keyspace", "metrictank", "cassandra keyspace to use for storing the metric data table")
	cassandraConsistency         = flag.String("cassandra-consistency", "one", "write consistency (any|one|two|three|quorum|all|local_quorum|each_quorum|local_one")
	cassandraHostSelectionPolicy = flag.String("cassandra-host-selection-policy", "tokenaware,hostpool-epsilon-greedy", "")
	cassandraTimeout             = flag.Int("cassandra-timeout", 1000, "cassandra timeout in milliseconds")
	cassandraConcurrency         = flag.Int("cassandra-concurrency", 20, "max number of concurrent reads to cassandra.")
	cassandraRetries             = flag.Int("cassandra-retries", 0, "how many times to retry a query before failing it")
	cassandraDisableHostLookup   = flag.Bool("cassandra-disable-host-lookup", false, "disable host lookup (useful if going through proxy)")
	cqlProtocolVersion           = flag.Int("cql-protocol-version", 4, "cql protocol version to use")

	cassandraSSL              = flag.Bool("cassandra-ssl", false, "enable SSL connection to cassandra")
	cassandraCaPath           = flag.String("cassandra-ca-path", "/etc/metrictank/ca.pem", "cassandra CA certificate path when using SSL")
	cassandraHostVerification = flag.Bool("cassandra-host-verification", true, "host (hostname and server cert) verification when using SSL")

	cassandraAuth     = flag.Bool("cassandra-auth", false, "enable cassandra authentication")
	cassandraUsername = flag.String("cassandra-username", "cassandra", "username for authentication")
	cassandraPassword = flag.String("cassandra-password", "cassandra", "password for authentication")

	startTs      = flag.Int("start-timestamp", 0, "timestamp at which to start, defaults to 0")
	endTs        = flag.Int("end-timestamp", math.MaxInt32, "timestamp at which to stop, defaults to int max")
	startToken   = flag.Int64("start-token", minToken, "token to start at (inclusive), defaults to math.MinInt64")
	endToken     = flag.Int64("end-token", maxToken, "token to stop at (inclusive), defaults to math.MaxInt64")
	numThreads   = flag.Int("threads", 1, "number of workers to use to process data")
	maxBatchSize = flag.Int("max-batch-size", 10, "max number of queries per batch")

	idxTable   = flag.String("idx-table", "metric_idx", "idx table in cassandra")
	partitions = flag.String("partitions", "*", "process ids for these partitions (comma separated list of partition numbers or '*' for all)")

	progressRows = flag.Int("progress-rows", 1000000, "number of rows between progress output")

	verbose = flag.Bool("verbose", false, "show every record being processed")

	timeStarted    time.Time
	doneKeys       uint64
	doneRows       uint64
	partitionIdMap map[string]struct{}
)

func main() {
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "mt-store-cp [flags] table-in [table-out]")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Copies data in Cassandra to use another table (and possibly another cluster).")
		fmt.Fprintln(os.Stderr, "It is up to you to assure table-out exists before running this tool")
		fmt.Fprintln(os.Stderr, "This tool is EXPERIMENTAL and needs doublechecking whether data is successfully written to Cassandra")
		fmt.Fprintln(os.Stderr, "see https://github.com/grafana/metrictank/pull/909 for details")
		fmt.Fprintln(os.Stderr, "Please report good or bad experiences in the above ticket or in a new one")
		fmt.Println("Flags:")
		flag.PrintDefaults()
		os.Exit(-1)
	}
	flag.Parse()

	if flag.NArg() < 2 || flag.NArg() > 3 {
		flag.Usage()
		os.Exit(2)
	}

	tableIn, tableOut := flag.Arg(1), flag.Arg(1)
	if flag.NArg() == 3 {
		tableOut = flag.Arg(2)
	}

	if sourceCassandraAddrs == destCassandraAddrs && tableIn == tableOut {
		panic("Source and destination cannot be the same")
	}

	sourceSession, err := NewCassandraStore(sourceCassandraAddrs)

	if err != nil {
		panic(fmt.Sprintf("Failed to instantiate source cassandra: %s", err))
	}

	destSession, err := NewCassandraStore(destCassandraAddrs)

	if err != nil {
		panic(fmt.Sprintf("Failed to instantiate dest cassandra: %s", err))
	}

	update(sourceSession, destSession, tableIn, tableOut)
}

func NewCassandraStore(cassandraAddrs *string) (*gocql.Session, error) {
	cluster := gocql.NewCluster(strings.Split(*cassandraAddrs, ",")...)
	if *cassandraSSL {
		cluster.SslOpts = &gocql.SslOptions{
			CaPath:                 *cassandraCaPath,
			EnableHostVerification: *cassandraHostVerification,
		}
	}
	if *cassandraAuth {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: *cassandraUsername,
			Password: *cassandraPassword,
		}
	}
	cluster.DisableInitialHostLookup = *cassandraDisableHostLookup
	cluster.Consistency = gocql.ParseConsistency(*cassandraConsistency)
	cluster.Timeout = time.Duration(*cassandraTimeout) * time.Millisecond
	cluster.NumConns = *cassandraConcurrency
	cluster.ProtoVersion = *cqlProtocolVersion
	cluster.Keyspace = *cassandraKeyspace
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: *cassandraRetries}

	switch *cassandraHostSelectionPolicy {
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
		return nil, fmt.Errorf("unknown HostSelectionPolicy '%q'", *cassandraHostSelectionPolicy)
	}

	return cluster.CreateSession()
}

func fetchPartitionIds(sourceSession *gocql.Session) {
	if *partitions == "*" {
		return
	}
	log.Println("Fetching ids for partitions ", *partitions)
	partitionIdMap = make(map[string]struct{})
	partitionStrs := strings.Split(*partitions, ",")
	selectQuery := fmt.Sprintf("SELECT id FROM %s where partition=?", *idxTable)
	for _, p := range partitionStrs {
		if *verbose {
			log.Println("Fetching ids for partition ", p)
		}
		partition, err := strconv.Atoi(p)
		if err != nil {
			panic(fmt.Sprintf("Could not parse partition %q, error = %s", p, err))
		}
		keyItr := sourceSession.Query(selectQuery, partition).Iter()
		var key string
		for keyItr.Scan(&key) {
			partitionIdMap[key] = struct{}{}
			if len(partitionIdMap)%10000 == 0 {
				log.Println("Loading...", len(partitionIdMap), " ids processed, processing partition ", p)
			}
		}
		err = keyItr.Close()
		if err != nil {
			panic(fmt.Sprintf("Failed querying for partition key %q, error = %s", p, err))
		}
	}
}

func shouldProcessKey(key string) bool {
	if *partitions == "*" {
		return true
	}
	// Keys look like <org>.<id>_[rolluptype_rollupspan_]<epoch_month>
	// e.g. 1.ecbf02491cb225b0d3070dca52592469_630
	// or   1.ecbf02491cb225b0d3070dca52592469_max_3600_630
	id := strings.Split(key, "_")[0]
	_, ok := partitionIdMap[id]
	return ok
}

// completenessEstimate estimates completess of this process (as a number between 0 and 1)
// by inspecting a cassandra token. The data is ordered by token, so assuming a uniform distribution
// across the token space, we can estimate process.
// the token space is -9,223,372,036,854,775,808 through 9,223,372,036,854,775,807
// so for example, if we're working on token 3007409301797035962 then we're about 0.66 complete
func completenessEstimate(token int64) float64 {
	tokenRange := float64(*endToken) - float64(*startToken)
	tokensProcessed := float64(token) - float64(*startToken)

	return tokensProcessed / tokenRange
}

func roundToSeconds(d time.Duration) time.Duration {
	return d - (d % time.Second)
}

func printProgress(id int, token int64, doneRowsSnap uint64) {
	doneKeysSnap := atomic.LoadUint64(&doneKeys)
	completeness := completenessEstimate(token)
	timeElapsed := time.Since(timeStarted)

	// Scale up to scale down to avoid fractional
	ratioLeft := (1 - completeness) / completeness
	timeRemaining := time.Duration(float64(timeElapsed) * ratioLeft)
	rowsPerSec := doneRowsSnap / (uint64(1) + uint64(timeElapsed/time.Second))
	log.Printf("WORKING: id=%d processed %d keys, %d rows, last token = %d, %.1f%% complete, elapsed=%v, remaining=%v, rows/s=%d",
		id, doneKeysSnap, doneRowsSnap, token, completeness*100, roundToSeconds(timeElapsed), roundToSeconds(timeRemaining), rowsPerSec)
}

func publishBatchUntilSuccess(destSession *gocql.Session, batch *gocql.Batch) *gocql.Batch {
	if batch.Size() == 0 {
		return batch
	}

	for {
		err := destSession.ExecuteBatch(batch)
		if err == nil {
			break
		}
		fmt.Fprintf(os.Stderr, "ERROR: failed to publish batch, trying again. error = %q\n", err)
	}

	return destSession.NewBatch(gocql.UnloggedBatch)
}

func worker(id int, jobs <-chan string, wg *sync.WaitGroup, sourceSession, destSession *gocql.Session, startTime, endTime int, tableIn, tableOut string) {
	defer wg.Done()
	var token, ttl int64
	var ts int
	var data []byte
	var query string

	// Since we are operating on a single key at a time, all data should live in the same partition.
	// This means batch inserts will reduce round trips without falling into the trap described here:
	// https://docs.datastax.com/en/cql/3.1/cql/cql_using/useBatch.html
	batch := destSession.NewBatch(gocql.UnloggedBatch)

	selectQuery := fmt.Sprintf("SELECT token(key), ts, data, TTL(data) FROM %s where key=? AND ts>=? AND ts<?", tableIn)
	insertQuery := fmt.Sprintf("INSERT INTO %s (data, key, ts) values(?,?,?) USING TTL ?", tableOut)

	for key := range jobs {

		if !shouldProcessKey(key) {
			continue
		}

		rowsHandledLocally := uint64(0)
		iter := sourceSession.Query(selectQuery, key, startTime, endTime).Iter()
		for iter.Scan(&token, &ts, &data, &ttl) {

			if *verbose {
				log.Printf("id=%d processing rownum=%d table=%q key=%q ts=%d query=%q data='%x'\n", id, atomic.LoadUint64(&doneRows)+1, tableIn, key, ts, query, data)
			}

			batch.Query(insertQuery, data, key, ts, ttl)

			if batch.Size() >= *maxBatchSize {
				if *verbose {
					log.Printf("id=%d sending batch size=%d for key=%q ts=%d'\n", id, batch.Size(), key, ts)
				}
				batch = publishBatchUntilSuccess(destSession, batch)
			}

			rowsHandledLocally++
		}

		batch = publishBatchUntilSuccess(destSession, batch)

		// A little racy, but good enough for progress reporting
		doneRowsOld := doneRows
		doneRowsSnap := atomic.AddUint64(&doneRows, rowsHandledLocally)
		if (doneRowsOld / uint64(*progressRows)) < (doneRowsSnap / uint64(*progressRows)) {
			printProgress(id, token, doneRowsSnap)
		}

		err := iter.Close()
		if err != nil {
			doneKeysSnap := atomic.LoadUint64(&doneKeys)
			doneRowsSnap := atomic.LoadUint64(&doneRows)
			fmt.Fprintf(os.Stderr, "ERROR: id=%d failed querying %s: %q. processed %d keys, %d rows\n", id, tableIn, err, doneKeysSnap, doneRowsSnap)
		}
		atomic.AddUint64(&doneKeys, 1)
	}
}

func update(sourceSession, destSession *gocql.Session, tableIn, tableOut string) {
	// Get the list of ids that we care about
	fetchPartitionIds(sourceSession)

	// Kick off our threads
	jobs := make(chan string, 10000)

	var wg sync.WaitGroup
	wg.Add(*numThreads)
	for i := 0; i < *numThreads; i++ {
		go worker(i, jobs, &wg, sourceSession, destSession, *startTs, *endTs, tableIn, tableOut)
	}

	timeStarted = time.Now()

	lastToken := *startToken

	// Key grab retry loop
	for {
		keyItr := sourceSession.Query(fmt.Sprintf("SELECT distinct key, token(key) FROM %s where token(key) >= %d AND token(key) <= %d", tableIn, lastToken, *endToken)).Iter()

		var key string
		for keyItr.Scan(&key, &lastToken) {
			jobs <- key
		}

		err := keyItr.Close()
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: failed querying %s: %q. processed %d keys, %d rows\n", tableIn, err, doneKeys, doneRows)
		} else {
			break
		}
	}

	close(jobs)

	wg.Wait()
	log.Printf("DONE.  Processed %d keys, %d rows\n", doneKeys, doneRows)
}
