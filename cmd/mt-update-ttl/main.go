package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/grafana/metrictank/logger"
	"github.com/grafana/metrictank/stats"
	"github.com/grafana/metrictank/store/cassandra"
	"github.com/raintank/dur"
	log "github.com/sirupsen/logrus"
)

const maxToken = math.MaxInt64 // 9223372036854775807

var (
	doneKeys    uint64
	doneRows    uint64
	startTs     int
	endTs       int
	numThreads  int
	statusEvery int
	verbose     bool
)

func init() {
	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)
}

func main() {
	cfg := cassandra.CliConfig
	flag.StringVar(&cfg.Addrs, "cassandra-addrs", cfg.Addrs, "cassandra host (may be given multiple times as comma-separated list)")
	flag.StringVar(&cfg.Keyspace, "cassandra-keyspace", cfg.Keyspace, "cassandra keyspace to use for storing the metric data table")
	flag.StringVar(&cfg.Consistency, "cassandra-consistency", cfg.Consistency, "write consistency (any|one|two|three|quorum|all|local_quorum|each_quorum|local_one")
	flag.StringVar(&cfg.HostSelectionPolicy, "host-selection-policy", cfg.HostSelectionPolicy, "")
	flag.StringVar(&cfg.Timeout, "cassandra-timeout", cfg.Timeout, "cassandra timeout")
	flag.IntVar(&cfg.WriteConcurrency, "cassandra-concurrency", 20, "number of concurrent connections to cassandra.") // this will launch idle write goroutines which we don't need but we can clean this up later.
	flag.IntVar(&cfg.Retries, "cassandra-retries", cfg.Retries, "how many times to retry a query before failing it")
	flag.IntVar(&cfg.WindowFactor, "window-factor", cfg.WindowFactor, "size of compaction window relative to TTL")
	flag.IntVar(&cfg.CqlProtocolVersion, "cql-protocol-version", cfg.CqlProtocolVersion, "cql protocol version to use")
	flag.BoolVar(&cfg.CreateKeyspace, "create-keyspace", cfg.CreateKeyspace, "enable the creation of the keyspace and tables")
	flag.BoolVar(&cfg.SSL, "cassandra-ssl", cfg.SSL, "enable SSL connection to cassandra")
	flag.StringVar(&cfg.CaPath, "cassandra-ca-path", cfg.CaPath, "cassandra CA certificate path when using SSL")
	flag.BoolVar(&cfg.HostVerification, "cassandra-host-verification", cfg.HostVerification, "host (hostname and server cert) verification when using SSL")
	flag.BoolVar(&cfg.Auth, "cassandra-auth", cfg.Auth, "enable cassandra authentication")
	flag.StringVar(&cfg.Username, "cassandra-username", cfg.Username, "username for authentication")
	flag.StringVar(&cfg.Password, "cassandra-password", cfg.Password, "password for authentication")
	flag.StringVar(&cfg.SchemaFile, "schema-file", cfg.SchemaFile, "File containing the needed schemas in case database needs initializing")
	flag.BoolVar(&cfg.DisableInitialHostLookup, "cassandra-disable-initial-host-lookup", cfg.DisableInitialHostLookup, "instruct the driver to not attempt to get host info from the system.peers table")

	cfg.ReadConcurrency = 0
	cfg.ReadQueueSize = 0
	cfg.WriteQueueSize = 0

	flag.IntVar(&startTs, "start-timestamp", 0, "timestamp at which to start, defaults to 0")
	flag.IntVar(&endTs, "end-timestamp", math.MaxInt32, "timestamp at which to stop, defaults to int max")
	flag.IntVar(&numThreads, "threads", 10, "number of workers to use to process data")
	flag.IntVar(&statusEvery, "status-every", 100000, "print status every x keys")

	flag.BoolVar(&verbose, "verbose", false, "show every record being processed")

	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "mt-update-ttl [flags] ttl table-in [table-out]")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Adjusts the data in Cassandra to use a new TTL value. The TTL is applied counting from the timestamp of the data")
		fmt.Fprintln(os.Stderr, "If table-out not specified or same as table-in, will update in place. Otherwise will not touch input table and store results in table-out")
		fmt.Fprintln(os.Stderr, "Unless you disable create-keyspace, table-out is created when necessary")
		fmt.Fprintln(os.Stderr, "Not supported yet: for the per-ttl tables as of 0.7, automatically putting data in the right table")
		fmt.Println("Flags:")
		flag.PrintDefaults()
		os.Exit(-1)
	}
	flag.Parse()

	stats.NewDevnull() // make sure metrics don't pile up without getting discarded

	if flag.NArg() < 2 || flag.NArg() > 3 {
		flag.Usage()
		os.Exit(2)
	}

	ttl := dur.MustParseNDuration("ttl", flag.Arg(0))
	tableIn, tableOut := flag.Arg(1), flag.Arg(1)
	if flag.NArg() == 3 {
		tableOut = flag.Arg(2)
	}

	// note: cassandraStore will not be aware via its TTLTables attribute of the other, pre-existing tables,
	// only of the table we're copying to. but that's ok because we don't exercise any functionality that
	// needs that
	store, err := cassandra.NewCassandraStore(cassandra.CliConfig, []uint32{ttl})

	if err != nil {
		log.Fatalf("Failed to instantiate cassandra: %s", err)
	}

	update(store, int(ttl), tableIn, tableOut)
}

func getTTL(now, ts, ttl int) int {
	newTTL := ttl - (now - ts)

	// setting a TTL of 0 or negative, is equivalent to no TTL = keep forever.
	// we wouldn't want that to happen by accident.
	// this can happen when setting or shortening a TTL for already very old data.
	if newTTL < 1 {
		return 1
	}
	return newTTL
}

// completenessEstimate estimates completess of this process (as a number between 0 and 1)
// by inspecting a cassandra token. The data is ordered by token, so assuming a uniform distribution
// across the token space, we can estimate process.
// the token space is -9,223,372,036,854,775,808 through 9,223,372,036,854,775,807
// so for example, if we're working on token 3007409301797035962 then we're about 0.66 complete
func completenessEstimate(token int64) float64 {
	return ((float64(token) / float64(maxToken)) + 1) / 2
}

func worker(id int, jobs <-chan string, wg *sync.WaitGroup, store *cassandra.CassandraStore, startTime, endTime, ttl int, tableIn, tableOut string) {
	defer wg.Done()
	var token int64
	var ts int
	var data []byte
	var query string
	queryTpl := fmt.Sprintf("SELECT token(key), ts, data FROM %s where key=? AND ts>=? AND ts<?", tableIn)

	for key := range jobs {
		iter := store.Session.Query(queryTpl, key, startTime, endTime).Iter()
		for iter.Scan(&token, &ts, &data) {
			newTTL := getTTL(int(time.Now().Unix()), ts, ttl)
			if tableIn == tableOut {
				query = fmt.Sprintf("UPDATE %s USING TTL %d SET data = ? WHERE key = ? AND ts = ?", tableIn, newTTL)
			} else {
				query = fmt.Sprintf("INSERT INTO %s (data, key, ts) values(?,?,?) USING TTL %d", tableOut, newTTL)
			}
			if verbose {
				log.Infof("id=%d processing rownum=%d table=%q key=%q ts=%d query=%q data='%x'\n", id, atomic.LoadUint64(&doneRows)+1, tableIn, key, ts, query, data)
			}

			err := store.Session.Query(query, data, key, ts).Exec()
			if err != nil {
				log.Errorf("id=%d failed updating %s %s %d: %q", id, tableOut, key, ts, err)
			}

			doneRowsSnap := atomic.AddUint64(&doneRows, 1)
			if doneRowsSnap%uint64(statusEvery) == 0 {
				doneKeysSnap := atomic.LoadUint64(&doneKeys)
				completeness := completenessEstimate(token)
				log.Infof("WORKING: id=%d processed %d keys, %d rows. (last token: %d, completeness estimate %.1f%%)", id, doneKeysSnap, doneRowsSnap, token, completeness*100)
			}
		}
		err := iter.Close()
		if err != nil {
			doneKeysSnap := atomic.LoadUint64(&doneKeys)
			doneRowsSnap := atomic.LoadUint64(&doneRows)
			log.Errorf("id=%d failed querying %s: %q. processed %d keys, %d rows", id, tableIn, err, doneKeysSnap, doneRowsSnap)
		}
		atomic.AddUint64(&doneKeys, 1)
	}
}

func update(store *cassandra.CassandraStore, ttl int, tableIn, tableOut string) {

	keyItr := store.Session.Query(fmt.Sprintf("SELECT distinct key FROM %s", tableIn)).Iter()

	jobs := make(chan string, 100)

	var wg sync.WaitGroup
	wg.Add(numThreads)
	for i := 0; i < numThreads; i++ {
		go worker(i, jobs, &wg, store, startTs, endTs, ttl, tableIn, tableOut)
	}

	var key string
	for keyItr.Scan(&key) {
		jobs <- key
	}

	close(jobs)
	err := keyItr.Close()
	if err != nil {
		doneKeysSnap := atomic.LoadUint64(&doneKeys)
		doneRowsSnap := atomic.LoadUint64(&doneRows)
		log.Errorf("failed querying %s: %q. processed %d keys, %d rows", tableIn, err, doneKeysSnap, doneRowsSnap)
		wg.Wait()
		os.Exit(2)
	}

	wg.Wait()
	log.Infof("DONE.  Processed %d keys, %d rows", doneKeys, doneRows)
}
