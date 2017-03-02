package main

import (
	"flag"
	"fmt"
	"math"
	"net/http"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/dgryski/go-tsz"
	"github.com/gocql/gocql"
	"github.com/kisielk/whisper-go/whisper"
	"github.com/raintank/dur"
	"github.com/raintank/metrictank/api"
	"github.com/raintank/metrictank/cluster"
	"github.com/raintank/metrictank/cluster/partitioner"
	"github.com/raintank/metrictank/idx"
	"github.com/raintank/metrictank/idx/cassandra"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/mdata/chunk"
	"github.com/raintank/metrictank/mdata/chunk/archive"
)

var (
	exitOnError = flag.Bool(
		"exit-on-error",
		true,
		"Exit with a message when there's an error",
	)
	verbose = flag.Bool(
		"verbose",
		false,
		"Write logs to terminal",
	)
	fakeAvgAggregates = flag.Bool(
		"fake-avg-aggregates",
		true,
		"Generate sum/cnt series out of avg series to accomodate metrictank",
	)
	httpEndpoint = flag.String(
		"http-endpoint",
		"127.0.0.1:8080",
		"The http endpoint to listen on",
	)
	cassandraAddrs = flag.String(
		"cassandra-addrs",
		"localhost",
		"cassandra host (may be given multiple times as comma-separated list)",
	)
	cassandraKeyspace = flag.String(
		"cassandra-keyspace",
		"metrictank",
		"cassandra keyspace to use for storing the metric data table",
	)
	cassAddr = flag.String(
		"cass-addr",
		"localhost",
		"Address of cassandra host.",
	)
	ttlsStr = flag.String(
		"ttls",
		"35d",
		"list of ttl strings used by MT separated by ':'",
	)
	windowFactor = flag.Int(
		"window-factor",
		20,
		"the window factor be used when creating the metric table schema",
	)
	partitionScheme = flag.String(
		"partition-scheme",
		"bySeries",
		"method used for partitioning metrics. This should match the settings of tsdb-gw. (byOrg|bySeries)",
	)
	uriPath = flag.String(
		"uri-path",
		"/chunks",
		"the URI on which we expect chunks to get posted",
	)
	numPartitions = flag.Int(
		"num-partitions",
		1,
		"Number of Partitions",
	)
	GitHash   = "(none)"
	printLock sync.Mutex
)

type Server struct {
	Cluster     *gocql.ClusterConfig
	Session     *gocql.Session
	TTLTables   mdata.TTLTables
	Partitioner partitioner.Partitioner
	Index       idx.MetricIndex
}

func main() {
	cassandra.ConfigSetup()
	flag.Parse()

	cassCluster := gocql.NewCluster(*cassAddr)
	cassCluster.Consistency = gocql.ParseConsistency("one")
	cassCluster.Timeout = time.Second
	cassCluster.NumConns = 2
	cassCluster.ProtoVersion = 4
	cassCluster.Keyspace = *cassandraKeyspace

	session, err := cassCluster.CreateSession()
	if err != nil {
		throwError(fmt.Sprintf("Failed to create cassandra session: %s", err))
	}

	splits := strings.Split(*ttlsStr, ":")
	ttls := make([]uint32, 0)
	for _, split := range splits {
		ttls = append(ttls, dur.MustParseUNsec("ttl", split))
	}
	ttlTables := mdata.GetTTLTables(ttls, *windowFactor, mdata.Table_name_format)

	p, err := partitioner.NewKafka(*partitionScheme)
	if err != nil {
		throwError(fmt.Sprintf("Failed to instantiate partitioner: %s", err))
	}

	server := &Server{
		Cluster:     cassCluster,
		Session:     session,
		TTLTables:   ttlTables,
		Partitioner: p,
		Index:       cassandra.New(),
	}
	cluster.Init("mt-whisper-importer-writer", GitHash, time.Now(), "http", int(80))
	server.Index.Init()

	http.HandleFunc(*uriPath, server.chunksHandler)

	log(fmt.Sprintf("Listening on %s", *httpEndpoint))
	err = http.ListenAndServe(*httpEndpoint, nil)
	if err != nil {
		throwError(fmt.Sprintf("Error creating listener: %s", err))
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

func (s *Server) chunksHandler(w http.ResponseWriter, req *http.Request) {
	metric := archive.NewMetricFromEncoded(req.Body)
	log("Handling new metric")

	if len(metric.Archives) == 0 {
		throwError("Metric has no archives")
	}

	avg := false
	if metric.Metadata.AggregationMethod == whisper.AggregationAverage {
		avg = true
	}

	partition, err := s.Partitioner.Partition(&metric.MetricData, int32(*numPartitions))
	err = s.Index.AddOrUpdate(&metric.MetricData, partition)
	if err != nil {
		throwError(fmt.Sprintf("Error updating metric index: %s", err))
	}

	for archiveIdx, a := range metric.Archives {
		archiveTTL := a.ArchiveInfo.SecondsPerPoint * a.ArchiveInfo.Points
		tableTTL := s.selectTableByTTL(archiveTTL)
		entry, ok := s.TTLTables[tableTTL]
		if !ok {
			throwError(fmt.Sprintf("Failed to find table with ttl %d in %+v", tableTTL, s.TTLTables))
		}
		tableName := entry.Table

		if !avg || archiveIdx == 0 || !*fakeAvgAggregates {
			log(fmt.Sprintf(
				"inserting %d chunks of archive %d with ttl %d into table %s with ttl %d and key %s",
				len(a.Chunks), archiveIdx, archiveTTL, tableName, tableTTL, a.RowKey,
			))
			s.insertChunk(tableName, a.RowKey, tableTTL, a.Chunks)
		} else {
			// averaged archives are a special case because mt doesn't store them as such.
			// mt reconstructs the averages on the fly from the sum and cnt series, so we need
			// to generate these two series out of raw averaged data by multiplying each point
			// with the aggregation span and storing the result as sum, cnt is the aggregation span.
			type sumCntChunks struct {
				sum *chunk.Chunk
				cnt *chunk.Chunk
			}

			// seconds per point is assumed to be the aggregation span
			aggSpan := a.ArchiveInfo.SecondsPerPoint

			sumArchive := make(archive.ArchiveOfChunks)
			cntArchive := make(archive.ArchiveOfChunks)
			for T0, c := range a.Chunks {
				sum := chunk.New(T0)
				cnt := chunk.New(T0)

				it, err := tsz.NewIterator(c.Bytes)
				if err != nil {
					throwError(fmt.Sprintf("failed to get iterator from chunk: %s", err))
				}
				for it.Next() {
					ts, val := it.Values()
					cnt.Push(ts, float64(aggSpan))
					sum.Push(ts, val*float64(aggSpan))
				}

				cnt.Finish()
				sum.Finish()

				sumArchive[T0] = archive.MetricChunk{ChunkSpan: aggSpan, Bytes: sum.Bytes()}
				cntArchive[T0] = archive.MetricChunk{ChunkSpan: aggSpan, Bytes: cnt.Bytes()}
			}

			cntId := api.AggMetricKey(metric.MetricData.Id, "cnt", aggSpan)
			sumId := api.AggMetricKey(metric.MetricData.Id, "sum", aggSpan)

			log(fmt.Sprintf(
				"inserting 2 archives of %d chunks per archive with ttl %d into table %s with ttl %d and keys %s/%s",
				len(a.Chunks), archiveTTL, tableName, tableTTL, cntId, sumId,
			))

			s.insertChunk(tableName, cntId, tableTTL, cntArchive)
			s.insertChunk(tableName, sumId, tableTTL, sumArchive)
		}
	}
}

func (s *Server) insertChunk(table, id string, ttl uint32, chunks archive.ArchiveOfChunks) {
	query := fmt.Sprintf("INSERT INTO %s (key, ts, data) values (?,?,?) USING TTL %d", table, ttl)
	for t0, chunk := range chunks {
		row_key := fmt.Sprintf("%s_%d", id, t0/mdata.Month_sec)
		err := s.Session.Query(query, row_key, t0, mdata.PrepareChunkData(chunk.ChunkSpan, chunk.Bytes)).Exec()
		if err != nil {
			throwError(fmt.Sprintf("Error in query: %s", err))
		}
	}
}

func (s *Server) selectTableByTTL(ttl uint32) uint32 {
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
		throwError(fmt.Sprintf("No Table found that can hold TTL %d", ttl))
	}

	return selectedTTL
}
