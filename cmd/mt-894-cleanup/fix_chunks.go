package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gocql/gocql"
	hostpool "github.com/hailocab/go-hostpool"
)

var (
	cassandraAddrs               = flag.String("cassandra-addrs", "localhost", "cassandra host (may be given multiple times as comma-separated list)")
	cassandraKeyspace            = flag.String("cassandra-keyspace", "metrictank", "cassandra keyspace to use for storing the metric data table")
	cassandraConsistency         = flag.String("cassandra-consistency", "one", "write consistency (any|one|two|three|quorum|all|local_quorum|each_quorum|local_one")
	cassandraTimeout             = flag.Int("cassandra-timeout", 1000, "cassandra timeout in milliseconds")
	cassandraConcurrency         = flag.Int("cassandra-concurrency", 20, "max number of concurrent reads to cassandra.")
	cassandraHostSelectionPolicy = flag.String("cassandra-host-selection-policy", "tokenaware,hostpool-epsilon-greedy", "")
	cassandraTable               = flag.String("cassandra-table", "metric_16384", "name of the table to fix")

	numPartitions = flag.Int("partitions", 128, "number of partitions to look for (can be higher than needed)")
	numThreads    = flag.Int("threads", 100, "number of workers to use to process data")
	monthStart    = flag.Int("month-start", 630, "starting month number row suffix")
	monthEnd      = flag.Int("month-end", 630, "ending month number row suffix")

	verbose = flag.Bool("verbose", false, "show every record being processed")

	scannedParts uint64
	doneKeys     uint64
	foundKeys    uint64
)

func main() {
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "fix_bad_chunks")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Rename aggregate chunks in a metric_16384 table that were named with the wrong agg span value (5 instead of 7200)")
		fmt.Fprintln(os.Stderr, "see https://github.com/grafana/metrictank/pull/894/ for more details.")
		fmt.Println("Flags:")
		flag.PrintDefaults()
		os.Exit(-1)
	}
	flag.Parse()

	session, err := NewCassandraStore()

	if err != nil {
		panic(fmt.Sprintf("Failed to instantiate cassandra: %s", err))
	}

	// add all of our partitions to a queue.  Workers will then pop them off and process all of the metrics
	// using that partition
	partitionChan := make(chan int, *numPartitions)
	for i := 0; i < *numPartitions; i++ {
		partitionChan <- i
	}
	close(partitionChan)

	// channel for sending metric keys from the index reader to the chunk updater
	keyChan := make(chan string, 1000000)

	var metricIdxReaderWg sync.WaitGroup
	var chunkUpdateWg sync.WaitGroup
	metricIdxReaderWg.Add(*numThreads)
	chunkUpdateWg.Add(*numThreads)

	for i := 0; i < *numThreads; i++ {
		go getMetricIDs(session, partitionChan, keyChan, &metricIdxReaderWg)
		go updateChunks(session, keyChan, &chunkUpdateWg)
	}

	done := make(chan struct{})
	go printProgressLoop(done)

	metricIdxReaderWg.Wait()
	log.Println("Finished reading metricIDs.  Found", foundKeys)
	close(keyChan)

	chunkUpdateWg.Wait()
	close(done)
	log.Println("Processing of chunks complete.")
	printProgress()
}

func printProgressLoop(done chan struct{}) {
	ticker := time.NewTicker(time.Second * 10)
	for {
		select {
		case <-ticker.C:
			printProgress()
		case <-done:
			return
		}
	}
}

func printProgress() {
	scannedParts := atomic.LoadUint64(&scannedParts)
	found := atomic.LoadUint64(&foundKeys)
	processed := atomic.LoadUint64(&doneKeys)
	log.Printf("Scanned %d partitions - Found %d keys - Processed %d keys", scannedParts, found, processed)
}

func NewCassandraStore() (*gocql.Session, error) {
	cluster := gocql.NewCluster(strings.Split(*cassandraAddrs, ",")...)
	cluster.Consistency = gocql.ParseConsistency(*cassandraConsistency)
	cluster.Timeout = time.Duration(*cassandraTimeout) * time.Millisecond
	cluster.NumConns = *cassandraConcurrency
	cluster.ProtoVersion = 4
	cluster.Keyspace = *cassandraKeyspace
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: 3}

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
		return nil, fmt.Errorf("unknown HostSelectionPolicy '%q'", cassandraHostSelectionPolicy)
	}

	cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(
		gocql.HostPoolHostPolicy(
			hostpool.NewEpsilonGreedy(nil, 0, &hostpool.LinearEpsilonValueCalculator{}),
		),
	)

	return cluster.CreateSession()
}

func updateChunks(session *gocql.Session, keyChan <-chan string, wg *sync.WaitGroup) {
	defer wg.Done()
	var data []byte
	var ts int
	var ttl int
	var query string
	queryTpl := fmt.Sprintf("SELECT ts, data, TTL(data) FROM %s where key=?", *cassandraTable)

	aggs := []string{"min", "max", "cnt", "sum", "lst"}
	var badKey string
	var replacementKey string

	for key := range keyChan {
		for month := *monthStart; month <= *monthEnd; month++ {
			for _, agg := range aggs {
				badKey = key + "_" + agg + "_5_" + strconv.Itoa(month)
				replacementKey = key + "_" + agg + "_7200_" + strconv.Itoa(month)
				iter := session.Query(queryTpl, badKey).Iter()
				for iter.Scan(&ts, &data, &ttl) {
					query = fmt.Sprintf("INSERT INTO %s (key, ts, data) values(?,?,?) USING TTL %d", *cassandraTable, ttl)
					err := session.Query(query, replacementKey, ts, data).Exec()
					if err != nil {
						fmt.Fprintf(os.Stderr, "ERROR: failed updating %s %d: %q", replacementKey, ts, err)
					}
				}
				err := iter.Close()
				if err != nil {
					fmt.Fprintf(os.Stderr, "ERROR: failed querying %s: %q.", badKey, err)
				}
			}
		}

		atomic.AddUint64(&doneKeys, 1)
	}
}

func getMetricIDs(session *gocql.Session, partitionChan <-chan int, keyChan chan<- string, wg *sync.WaitGroup) {
	defer wg.Done()
	// we only care about metrics that have been updated in the last 7days.
	tooOld := time.Now().AddDate(0, 0, -7).Unix()
	for partition := range partitionChan {
		log.Println("getMetricIDs: reading partition", partition)
		keyItr := session.Query("SELECT id, lastupdate from metric_idx where partition = ?", partition).Iter()

		var key string
		var lastUpdate int64
		for keyItr.Scan(&key, &lastUpdate) {
			if lastUpdate < tooOld {
				continue
			}
			keyChan <- key
			atomic.AddUint64(&foundKeys, 1)
		}
		err := keyItr.Close()
		if err != nil {
			log.Fatalf("getMetricIDs: failed to read from metric_idx table. %s", err.Error())
		}
		atomic.AddUint64(&scannedParts, 1)
	}
}
