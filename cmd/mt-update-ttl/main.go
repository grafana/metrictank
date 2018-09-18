package main

import (
	"flag"
	"fmt"
	"math"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/gocql/gocql"
	"github.com/grafana/metrictank/logger"
	"github.com/grafana/metrictank/store/cassandra"
	hostpool "github.com/hailocab/go-hostpool"
	"github.com/raintank/dur"
)

const maxToken = math.MaxInt64 // 9223372036854775807

var (
	cassandraAddrs               = flag.String("cassandra-addrs", "localhost", "cassandra host (may be given multiple times as comma-separated list)")
	cassandraKeyspace            = flag.String("cassandra-keyspace", "metrictank", "cassandra keyspace to use for storing the metric data table")
	cassandraConsistency         = flag.String("cassandra-consistency", "one", "write consistency (any|one|two|three|quorum|all|local_quorum|each_quorum|local_one")
	cassandraHostSelectionPolicy = flag.String("cassandra-host-selection-policy", "tokenaware,hostpool-epsilon-greedy", "")
	cassandraTimeout             = flag.String("cassandra-timeout", "1s", "cassandra timeout")
	cassandraConcurrency         = flag.Int("cassandra-concurrency", 20, "max number of concurrent reads to cassandra.")
	cassandraRetries             = flag.Int("cassandra-retries", 0, "how many times to retry a query before failing it")
	cqlProtocolVersion           = flag.Int("cql-protocol-version", 4, "cql protocol version to use")

	cassandraSSL              = flag.Bool("cassandra-ssl", false, "enable SSL connection to cassandra")
	cassandraCaPath           = flag.String("cassandra-ca-path", "/etc/metrictank/ca.pem", "cassandra CA certificate path when using SSL")
	cassandraHostVerification = flag.Bool("cassandra-host-verification", true, "host (hostname and server cert) verification when using SSL")

	cassandraAuth     = flag.Bool("cassandra-auth", false, "enable cassandra authentication")
	cassandraUsername = flag.String("cassandra-username", "cassandra", "username for authentication")
	cassandraPassword = flag.String("cassandra-password", "cassandra", "password for authentication")

	cassandraDisableInitialHostLookup = flag.Bool("cassandra-disable-initial-host-lookup", false, "instruct the driver to not attempt to get host info from the system.peers table")

	startTs    = flag.Int("start-timestamp", 0, "timestamp at which to start, defaults to 0")
	endTs      = flag.Int("end-timestamp", math.MaxInt32, "timestamp at which to stop, defaults to int max")
	numThreads = flag.Int("threads", 1, "number of workers to use to process data")

	verbose = flag.Bool("verbose", false, "show every record being processed")

	doneKeys uint64
	doneRows uint64
)

func main() {
	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	formatter.QuoteEmptyFields = true

	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)

	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "mt-update-ttl [flags] ttl table-in [table-out]")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Adjusts the data in Cassandra to use a new TTL value. The TTL is applied counting from the timestamp of the data")
		fmt.Fprintln(os.Stderr, "If table-out not specified or same as table-in, will update in place. Otherwise will not touch input table and store results in table-out")
		fmt.Fprintln(os.Stderr, "In that case, it is up to you to assure table-out exists before running this tool")
		fmt.Fprintln(os.Stderr, "Not supported yet: for the per-ttl tables as of 0.7, automatically putting data in the right table")
		fmt.Println("Flags:")
		flag.PrintDefaults()
		os.Exit(-1)
	}
	flag.Parse()

	if flag.NArg() < 2 || flag.NArg() > 3 {
		flag.Usage()
		os.Exit(2)
	}

	ttl := int(dur.MustParseNDuration("ttl", flag.Arg(0)))
	tableIn, tableOut := flag.Arg(1), flag.Arg(1)
	if flag.NArg() == 3 {
		tableOut = flag.Arg(2)
	}

	session, err := NewCassandraStore()

	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Panic("failed to instantiate cassandra")
	}

	update(session, ttl, tableIn, tableOut)
}

func NewCassandraStore() (*gocql.Session, error) {
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
	cluster.Consistency = gocql.ParseConsistency(*cassandraConsistency)
	cluster.Timeout = cassandra.ConvertTimeout(*cassandraTimeout, time.Millisecond)
	cluster.NumConns = *cassandraConcurrency
	cluster.ProtoVersion = *cqlProtocolVersion
	cluster.Keyspace = *cassandraKeyspace
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: *cassandraRetries}
	cluster.DisableInitialHostLookup = *cassandraDisableInitialHostLookup

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

func worker(id int, jobs <-chan string, wg *sync.WaitGroup, session *gocql.Session, startTime, endTime, ttl int, tableIn, tableOut string) {
	defer wg.Done()
	var token int64
	var ts int
	var data []byte
	var query string
	queryTpl := fmt.Sprintf("SELECT token(key), ts, data FROM %s where key=? AND ts>=? AND ts<?", tableIn)

	for key := range jobs {
		iter := session.Query(queryTpl, key, startTime, endTime).Iter()
		for iter.Scan(&token, &ts, &data) {
			newTTL := getTTL(int(time.Now().Unix()), ts, ttl)
			if tableIn == tableOut {
				query = fmt.Sprintf("UPDATE %s USING TTL %d SET data = ? WHERE key = ? AND ts = ?", tableIn, newTTL)
			} else {
				query = fmt.Sprintf("INSERT INTO %s (data, key, ts) values(?,?,?) USING TTL %d", tableOut, newTTL)
			}
			if *verbose {
				log.WithFields(log.Fields{
					"id":    id,
					"row":   atomic.LoadUint64(&doneRows),
					"table": tableIn,
					"key":   key,
					"ts":    ts,
					"query": query,
					"data":  data,
				}).Info("processing")
			}

			err := session.Query(query, data, key, ts).Exec()
			if err != nil {
				log.WithFields(log.Fields{
					"id":    id,
					"table": tableOut,
					"key":   key,
					"ts":    ts,
					"error": err,
				}).Error("failed updating")
			}

			doneRowsSnap := atomic.AddUint64(&doneRows, 1)
			if doneRowsSnap%10000 == 0 {
				doneKeysSnap := atomic.LoadUint64(&doneKeys)
				completeness := completenessEstimate(token)
				log.WithFields(log.Fields{
					"id":             id,
					"keys.processed": doneKeysSnap,
					"rows.processed": doneRowsSnap,
					"last.token":     token,
					"completed":      strconv.FormatFloat((completeness*100), 'f', 1, 64) + "%",
				}).Info("working")
			}
		}
		err := iter.Close()
		if err != nil {
			doneKeysSnap := atomic.LoadUint64(&doneKeys)
			doneRowsSnap := atomic.LoadUint64(&doneRows)
			log.WithFields(log.Fields{
				"id":             id,
				"table":          tableIn,
				"error":          err,
				"keys.processed": doneKeysSnap,
				"rows.processed": doneRowsSnap,
			}).Error("failed querying")
		}
		atomic.AddUint64(&doneKeys, 1)
	}
}

func update(session *gocql.Session, ttl int, tableIn, tableOut string) {

	keyItr := session.Query(fmt.Sprintf("SELECT distinct key FROM %s", tableIn)).Iter()

	jobs := make(chan string, 100)

	var wg sync.WaitGroup
	wg.Add(*numThreads)
	for i := 0; i < *numThreads; i++ {
		go worker(i, jobs, &wg, session, *startTs, *endTs, ttl, tableIn, tableOut)
	}

	var key string
	for keyItr.Scan(&key) {
		jobs <- key
	}

	close(jobs)
	err := keyItr.Close()
	if err != nil {
		log.WithFields(log.Fields{
			"table":          tableIn,
			"error":          err,
			"keys.processed": doneKeys,
			"rows.processed": doneRows,
		}).Error("failed querying")
		wg.Wait()
		os.Exit(2)
	}

	wg.Wait()
	log.WithFields(log.Fields{
		"keys.processed": doneKeys,
		"rows.processed": doneRows,
	}).Info("done")
}
