package main

import (
	"errors"
	"flag"
	"fmt"
	"math"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/gocql/gocql"
	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/cluster/partitioner"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/idx/cassandra"
	"github.com/grafana/metrictank/logger"
	"github.com/grafana/metrictank/mdata/chunk"
	"github.com/grafana/metrictank/mdata/chunk/archive"
	cassandraStore "github.com/grafana/metrictank/store/cassandra"
	"github.com/raintank/dur"
	"github.com/raintank/schema"
	log "github.com/sirupsen/logrus"
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
		"More detailed logging",
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
	partitionScheme = globalFlags.String(
		"partition-scheme",
		"bySeries",
		"method used for partitioning metrics. This should match the settings of tsdb-gw. (byOrg|bySeries|bySeriesWithTags)",
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
	overwriteChunks = globalFlags.Bool(
		"overwrite-chunks",
		true,
		"If true existing chunks may be overwritten",
	)

	version = "(none)"
)

type Server struct {
	Session     *gocql.Session
	TTLTables   cassandraStore.TTLTables
	Partitioner partitioner.Partitioner
	Index       idx.MetricIndex
	HTTPServer  *http.Server
}

func init() {
	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)
}

func main() {
	storeConfig := cassandraStore.NewStoreConfig()
	// we don't use the cassandraStore's writeQueue, so we hard code this to 0.
	storeConfig.WriteQueueSize = 0

	// flags from cassandra/config.go, Cassandra
	globalFlags.StringVar(&storeConfig.Addrs, "cassandra-addrs", storeConfig.Addrs, "cassandra host (may be given multiple times as comma-separated list)")
	globalFlags.StringVar(&storeConfig.Keyspace, "cassandra-keyspace", storeConfig.Keyspace, "cassandra keyspace to use for storing the metric data table")
	globalFlags.StringVar(&storeConfig.Consistency, "cassandra-consistency", storeConfig.Consistency, "write consistency (any|one|two|three|quorum|all|local_quorum|each_quorum|local_one")
	globalFlags.StringVar(&storeConfig.HostSelectionPolicy, "cassandra-host-selection-policy", storeConfig.HostSelectionPolicy, "")
	globalFlags.StringVar(&storeConfig.Timeout, "cassandra-timeout", storeConfig.Timeout, "cassandra timeout")
	globalFlags.IntVar(&storeConfig.ReadConcurrency, "cassandra-read-concurrency", storeConfig.ReadConcurrency, "max number of concurrent reads to cassandra.")
	globalFlags.IntVar(&storeConfig.WriteConcurrency, "cassandra-write-concurrency", storeConfig.WriteConcurrency, "max number of concurrent writes to cassandra.")
	globalFlags.IntVar(&storeConfig.ReadQueueSize, "cassandra-read-queue-size", storeConfig.ReadQueueSize, "max number of outstanding reads before reads will be dropped. This is important if you run queries that result in many reads in parallel.")
	//flag.IntVar(&storeConfig.WriteQueueSize, "write-queue-size", storeConfig.WriteQueueSize, "write queue size per cassandra worker. should be large engough to hold all at least the total number of series expected, divided by how many workers you have")
	globalFlags.IntVar(&storeConfig.Retries, "cassandra-retries", storeConfig.Retries, "how many times to retry a query before failing it")
	globalFlags.IntVar(&storeConfig.WindowFactor, "cassandra-window-factor", storeConfig.WindowFactor, "size of compaction window relative to TTL")
	globalFlags.StringVar(&storeConfig.OmitReadTimeout, "cassandra-omit-read-timeout", storeConfig.OmitReadTimeout, "if a read is older than this, it will directly be omitted without executing")
	globalFlags.IntVar(&storeConfig.CqlProtocolVersion, "cql-protocol-version", storeConfig.CqlProtocolVersion, "cql protocol version to use")
	globalFlags.BoolVar(&storeConfig.CreateKeyspace, "cassandra-create-keyspace", storeConfig.CreateKeyspace, "enable the creation of the mdata keyspace and tables, only one node needs this")
	globalFlags.BoolVar(&storeConfig.DisableInitialHostLookup, "cassandra-disable-initial-host-lookup", storeConfig.DisableInitialHostLookup, "instruct the driver to not attempt to get host info from the system.peers table")
	globalFlags.BoolVar(&storeConfig.SSL, "cassandra-ssl", storeConfig.SSL, "enable SSL connection to cassandra")
	globalFlags.StringVar(&storeConfig.CaPath, "cassandra-ca-path", storeConfig.CaPath, "cassandra CA certificate path when using SSL")
	globalFlags.BoolVar(&storeConfig.HostVerification, "cassandra-host-verification", storeConfig.HostVerification, "host (hostname and server cert) verification when using SSL")
	globalFlags.BoolVar(&storeConfig.Auth, "cassandra-auth", storeConfig.Auth, "enable cassandra authentication")
	globalFlags.StringVar(&storeConfig.Username, "cassandra-username", storeConfig.Username, "username for authentication")
	globalFlags.StringVar(&storeConfig.Password, "cassandra-password", storeConfig.Password, "password for authentication")
	globalFlags.StringVar(&storeConfig.SchemaFile, "cassandra-schema-file", storeConfig.SchemaFile, "File containing the needed schemas in case database needs initializing")

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
		fmt.Println("mt-whisper-importer-writer -cassandra-addrs=192.168.0.1 -cassandra-keyspace=mydata -exit-on-error=true -fake-avg-aggregates=true -http-endpoint=0.0.0.0:8080 -num-partitions=8 -partition-scheme=bySeries -ttls=8d,2y -uri-path=/chunks -verbose=true -cassandra-window-factor=20 cass -hosts=192.168.0.1:9042 -keyspace=mydata")
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
	cassandra.CliConfig.Enabled = true
	cassandra.ConfigProcess()

	if *verbose {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}

	store, err := cassandraStore.NewCassandraStore(storeConfig, nil)
	if err != nil {
		panic(fmt.Sprintf("Failed to initialize cassandra: %q", err))
	}

	splits := strings.Split(*ttlsStr, ",")
	ttls := make([]uint32, 0)
	for _, split := range splits {
		ttls = append(ttls, dur.MustParseNDuration("ttl", split))
	}
	ttlTables := cassandraStore.GetTTLTables(ttls, storeConfig.WindowFactor, cassandraStore.Table_name_format)

	p, err := partitioner.NewKafka(*partitionScheme)
	if err != nil {
		panic(fmt.Sprintf("Failed to instantiate partitioner: %q", err))
	}

	cluster.Init("mt-whisper-importer-writer", version, time.Now(), "http", int(80))

	server := &Server{
		Session:     store.Session,
		TTLTables:   ttlTables,
		Partitioner: p,
		Index:       cassandra.New(cassandra.CliConfig),
		HTTPServer: &http.Server{
			Addr:        *httpEndpoint,
			ReadTimeout: 10 * time.Minute,
		},
	}
	server.Index.Init()

	http.HandleFunc(*uriPath, server.chunksHandler)
	http.HandleFunc("/healthz", server.healthzHandler)

	log.Infof("Listening on %q", *httpEndpoint)
	err = http.ListenAndServe(*httpEndpoint, nil)
	if err != nil {
		panic(fmt.Sprintf("Error creating listener: %q", err))
	}
}

func throwError(msg string) {
	msg = fmt.Sprintf("%s\n", msg)
	if *exitOnError {
		log.Panic(msg)
	} else {
		log.Error(msg)
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

	log.Debugf(
		"Receiving Id:%s OrgId:%d Name:%s AggMeth:%d ArchCnt:%d",
		metric.MetricData.Id, metric.MetricData.OrgId, metric.MetricData.Name, metric.AggregationMethod, len(metric.Archives))

	if len(metric.Archives) == 0 {
		throwError("Metric has no archives")
		return
	}
	mkey, err := schema.MKeyFromString(metric.MetricData.Id)
	if err != nil {
		throwError(fmt.Sprintf("Invalid MetricData.Id: %s", err))
	}

	partition, err := s.Partitioner.Partition(&metric.MetricData, int32(*numPartitions))
	if err != nil {
		throwError(fmt.Sprintf("Error partitioning: %q", err))
		return
	}
	s.Index.AddOrUpdate(mkey, &metric.MetricData, partition)

	for archiveIdx, a := range metric.Archives {
		archiveTTL := a.SecondsPerPoint * a.Points
		tableTTL, err := s.selectTableByTTL(archiveTTL)
		if err != nil {
			throwError(fmt.Sprintf("Failed to select table for ttl %d in %+v: %q", archiveTTL, s.TTLTables, err))
			return
		}
		table, ok := s.TTLTables[tableTTL]
		if !ok {
			throwError(fmt.Sprintf("Failed to get selected table %d in %+v", tableTTL, s.TTLTables))
			return
		}
		log.Debugf(
			"inserting %d chunks of archive %d with ttl %d into table %s with ttl %d and key %s",
			len(a.Chunks), archiveIdx, archiveTTL, table.Name, tableTTL, a.RowKey,
		)
		s.insertChunks(table.Name, a.RowKey, tableTTL, a.Chunks)
	}
}

func (s *Server) insertChunks(table, id string, ttl uint32, itergens []chunk.IterGen) {
	var query string
	if *overwriteChunks {
		query = fmt.Sprintf("INSERT INTO %s (key, ts, data) values (?,?,?) USING TTL %d", table, ttl)
	} else {
		query = fmt.Sprintf("INSERT INTO %s (key, ts, data) values (?,?,?) IF NOT EXISTS USING TTL %d", table, ttl)
	}
	log.Debug(query)
	for _, ig := range itergens {
		rowKey := fmt.Sprintf("%s_%d", id, ig.T0/cassandraStore.Month_sec)
		success := false
		attempts := 0
		for !success {
			err := s.Session.Query(query, rowKey, ig.T0, ig.B).Exec()
			if err != nil {
				if (attempts % 20) == 0 {
					log.Warnf("CS: failed to save chunk to cassandra after %d attempts. %s", attempts+1, err)
				}
				sleepTime := 100 * attempts
				if sleepTime > 2000 {
					sleepTime = 2000
				}
				time.Sleep(time.Duration(sleepTime) * time.Millisecond)
				attempts++
			} else {
				success = true
			}
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
