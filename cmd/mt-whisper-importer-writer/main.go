package main

import (
	"errors"
	"flag"
	"fmt"
	"math"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/gocql/gocql"
	"github.com/raintank/dur"
	"github.com/raintank/metrictank/cluster"
	"github.com/raintank/metrictank/cluster/partitioner"
	"github.com/raintank/metrictank/idx"
	"github.com/raintank/metrictank/idx/cassandra"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/mdata/chunk"
	"github.com/raintank/metrictank/mdata/chunk/archive"
)

var (
	globalFlags = flag.NewFlagSet("global config flags", flag.ExitOnError)

	exitOnError = globalFlags.Bool(
		"exit-on-error",
		true,
		"Exit with a message when there's an error",
	)
	verbose = globalFlags.Bool(
		"verbose",
		false,
		"Write logs to terminal",
	)
	fakeAvgAggregates = globalFlags.Bool(
		"fake-avg-aggregates",
		true,
		"Generate sum/cnt series out of avg series to accommodate metrictank",
	)
	httpEndpoint = globalFlags.String(
		"http-endpoint",
		"127.0.0.1:8080",
		"The http endpoint to listen on",
	)
	ttlsStr = globalFlags.String(
		"ttls",
		"35d",
		"list of ttl strings used by MT separated by ','",
	)
	windowFactor = globalFlags.Int(
		"window-factor",
		20,
		"the window factor be used when creating the metric table schema",
	)
	partitionScheme = globalFlags.String(
		"partition-scheme",
		"bySeries",
		"method used for partitioning metrics. This should match the settings of tsdb-gw. (byOrg|bySeries)",
	)
	uriPath = globalFlags.String(
		"uri-path",
		"/chunks",
		"the URI on which we expect chunks to get posted",
	)
	numPartitions = globalFlags.Int(
		"num-partitions",
		1,
		"Number of Partitions",
	)

	cassandraAddrs               = globalFlags.String("cassandra-addrs", "localhost", "cassandra host (may be given multiple times as comma-separated list)")
	cassandraKeyspace            = globalFlags.String("cassandra-keyspace", "raintank", "cassandra keyspace to use for storing the metric data table")
	cassandraConsistency         = globalFlags.String("cassandra-consistency", "one", "write consistency (any|one|two|three|quorum|all|local_quorum|each_quorum|local_one")
	cassandraHostSelectionPolicy = globalFlags.String("cassandra-host-selection-policy", "tokenaware,hostpool-epsilon-greedy", "")
	cassandraTimeout             = globalFlags.Int("cassandra-timeout", 1000, "cassandra timeout in milliseconds")
	cassandraReadConcurrency     = globalFlags.Int("cassandra-read-concurrency", 20, "max number of concurrent reads to cassandra.")
	cassandraReadQueueSize       = globalFlags.Int("cassandra-read-queue-size", 100, "max number of outstanding reads before blocking. value doesn't matter much")
	cassandraRetries             = globalFlags.Int("cassandra-retries", 0, "how many times to retry a query before failing it")
	cqlProtocolVersion           = globalFlags.Int("cql-protocol-version", 4, "cql protocol version to use")
	cassandraCreateKeyspace      = globalFlags.Bool("cassandra-create-keyspace", true, "enable the creation of the metrictank keyspace")

	cassandraSSL              = globalFlags.Bool("cassandra-ssl", false, "enable SSL connection to cassandra")
	cassandraCaPath           = globalFlags.String("cassandra-ca-path", "/etc/metrictank/ca.pem", "cassandra CA certificate path when using SSL")
	cassandraHostVerification = globalFlags.Bool("cassandra-host-verification", true, "host (hostname and server cert) verification when using SSL")

	cassandraAuth     = globalFlags.Bool("cassandra-auth", false, "enable cassandra authentication")
	cassandraUsername = globalFlags.String("cassandra-username", "cassandra", "username for authentication")
	cassandraPassword = globalFlags.String("cassandra-password", "cassandra", "password for authentication")

	GitHash   = "(none)"
	printLock sync.Mutex
)

type Server struct {
	Session     *gocql.Session
	TTLTables   mdata.TTLTables
	Partitioner partitioner.Partitioner
	Index       idx.MetricIndex
}

func main() {
	cassFlags := cassandra.ConfigSetup()

	flag.Usage = func() {
		fmt.Println("mt-whisper-importer-writer")
		fmt.Println()
		fmt.Println("Opens an endpoint to send data to, which then gets stored in the MT internal DB(s)")
		fmt.Println()
		fmt.Printf("Usage:\n\n")
		fmt.Printf("  mt-whisper-importer-writer [global config flags] <idxtype> [idx config flags] \n\n")
		fmt.Printf("global config flags:\n\n")
		globalFlags.PrintDefaults()
		fmt.Println()
		fmt.Printf("idxtype: only 'cass' supported for now\n\n")
		fmt.Printf("cass config flags:\n\n")
		cassFlags.PrintDefaults()
		fmt.Println()
		fmt.Println("EXAMPLES:")
		fmt.Println("mt-whisper-importer-writer -cassandra-addrs=192.168.0.1 -cassandra-keyspace=mydata -exit-on-error=true -fake-avg-aggregates=true -http-endpoint=0.0.0.0:8080 -num-partitions=8 -partition-scheme=bySeries -ttls=8d,2y -uri-path=/chunks -verbose=true -window-factor=20 cass -hosts=192.168.0.1:9042 -keyspace=mydata")
	}

	if len(os.Args) == 2 && (os.Args[1] == "-h" || os.Args[1] == "--help") {
		flag.Usage()
		os.Exit(0)
	}

	var cassI int
	for i, v := range os.Args {
		if v == "cass" {
			cassI = i
		}
	}
	if cassI == 0 {
		fmt.Println("only indextype 'cass' supported")
		flag.Usage()
		os.Exit(1)
	}

	globalFlags.Parse(os.Args[1:cassI])
	cassFlags.Parse(os.Args[cassI+1 : len(os.Args)])
	cassandra.Enabled = true

	store, err := mdata.NewCassandraStore(*cassandraAddrs, *cassandraKeyspace, *cassandraConsistency, *cassandraCaPath, *cassandraUsername, *cassandraPassword, *cassandraHostSelectionPolicy, *cassandraTimeout, *cassandraReadConcurrency, *cassandraReadConcurrency, *cassandraReadQueueSize, 0, *cassandraRetries, *cqlProtocolVersion, *windowFactor, 60, *cassandraSSL, *cassandraAuth, *cassandraHostVerification, *cassandraCreateKeyspace, nil)
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize cassandra: %q", err))
	}

	splits := strings.Split(*ttlsStr, ",")
	ttls := make([]uint32, 0)
	for _, split := range splits {
		ttls = append(ttls, dur.MustParseNDuration("ttl", split))
	}
	ttlTables := mdata.GetTTLTables(ttls, *windowFactor, mdata.Table_name_format)

	p, err := partitioner.NewKafka(*partitionScheme)
	if err != nil {
		panic(fmt.Sprintf("Failed to instantiate partitioner: %q", err))
	}

	server := &Server{
		Session:     store.Session,
		TTLTables:   ttlTables,
		Partitioner: p,
		Index:       cassandra.New(),
	}
	cluster.Init("mt-whisper-importer-writer", GitHash, time.Now(), "http", int(80))
	server.Index.Init()

	http.HandleFunc(*uriPath, server.chunksHandler)
	http.HandleFunc("/healthz", server.healthzHandler)

	log(fmt.Sprintf("Listening on %q", *httpEndpoint))
	err = http.ListenAndServe(*httpEndpoint, nil)
	if err != nil {
		panic(fmt.Sprintf("Error creating listener: %q", err))
	}
}

func throwError(msg string) {
	msg = fmt.Sprintf("%s\n", msg)
	if *exitOnError {
		panic(msg)
	} else {
		printLock.Lock()
		fmt.Fprintln(os.Stderr, msg)
		printLock.Unlock()
	}
}

func log(msg string) {
	if *verbose {
		printLock.Lock()
		fmt.Println(msg)
		printLock.Unlock()
	}
}

func (s *Server) healthzHandler(w http.ResponseWriter, req *http.Request) {
	w.Write([]byte("ok"))
}

func (s *Server) chunksHandler(w http.ResponseWriter, req *http.Request) {
	metric := &archive.Metric{}
	err := metric.UnmarshalCompressed(req.Body)
	if err != nil {
		throwError(fmt.Sprintf("Error decoding metric stream: %q", err))
		return
	}
	log("Handling new metric")

	if len(metric.Archives) == 0 {
		throwError("Metric has no archives")
		return
	}

	partition, err := s.Partitioner.Partition(&metric.MetricData, int32(*numPartitions))
	if err != nil {
		throwError(fmt.Sprintf("Error partitioning: %q", err))
		return
	}
	s.Index.AddOrUpdate(&metric.MetricData, partition)

	for archiveIdx, a := range metric.Archives {
		archiveTTL := a.SecondsPerPoint * a.Points
		tableTTL, err := s.selectTableByTTL(archiveTTL)
		if err != nil {
			throwError(fmt.Sprintf("Failed to select table for ttl %d in %+v: %q", archiveTTL, s.TTLTables, err))
			return
		}
		entry, ok := s.TTLTables[tableTTL]
		if !ok {
			throwError(fmt.Sprintf("Failed to get selected table %d in %+v", tableTTL, s.TTLTables))
			return
		}
		tableName := entry.Table

		log(fmt.Sprintf(
			"inserting %d chunks of archive %d with ttl %d into table %s with ttl %d and key %s",
			len(a.Chunks), archiveIdx, archiveTTL, tableName, tableTTL, a.RowKey,
		))
		s.insertChunks(tableName, a.RowKey, tableTTL, a.Chunks)
	}
}

func (s *Server) insertChunks(table, id string, ttl uint32, itergens []chunk.IterGen) {
	query := fmt.Sprintf("INSERT INTO %s (key, ts, data) values (?,?,?) USING TTL %d", table, ttl)
	for _, ig := range itergens {
		rowKey := fmt.Sprintf("%s_%d", id, ig.Ts/mdata.Month_sec)
		err := s.Session.Query(query, rowKey, ig.Ts, mdata.PrepareChunkData(ig.Span, ig.Bytes())).Exec()
		if err != nil {
			throwError(fmt.Sprintf("Error in query: %q", err))
		}
	}
}

func (s *Server) selectTableByTTL(ttl uint32) (uint32, error) {
	selectedTTL := uint32(math.MaxUint32)

	// find the table with the smallest TTL that is at least equal to archiveTTL
	for tableTTL := range s.TTLTables {
		if tableTTL >= ttl {
			if selectedTTL > tableTTL {
				selectedTTL = tableTTL
			}
		}
	}

	// we have not found a table that can accommodate the requested ttl
	if selectedTTL == math.MaxUint32 {
		return 0, errors.New(fmt.Sprintf("No Table found that can hold TTL %d", ttl))
	}

	return selectedTTL, nil
}
