package main

import (
	"flag"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/grafana/globalconf"
	"github.com/raintank/dur"
	"github.com/raintank/schema"
	log "github.com/sirupsen/logrus"

	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/cluster/partitioner"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/idx/bigtable"
	"github.com/grafana/metrictank/idx/cassandra"
	"github.com/grafana/metrictank/logger"
	"github.com/grafana/metrictank/mdata"
	bigTableStore "github.com/grafana/metrictank/store/bigtable"
	cassandraStore "github.com/grafana/metrictank/store/cassandra"
)

var (
	confFile        = flag.String("config", "/etc/metrictank/metrictank.ini", "configuration file path")
	exitOnError     = flag.Bool("exit-on-error", false, "Exit with a message when there's an error")
	httpEndpoint    = flag.String("http-endpoint", "127.0.0.1:8080", "The http endpoint to listen on")
	ttlsStr         = flag.String("ttls", "35d", "list of ttl strings used by MT separated by ','")
	partitionScheme = flag.String("partition-scheme", "bySeries", "method used for partitioning metrics. This should match the settings of tsdb-gw. (byOrg|bySeries)")
	uriPath         = flag.String("uri-path", "/chunks", "the URI on which we expect chunks to get posted")
	numPartitions   = flag.Int("num-partitions", 1, "Number of Partitions")
	logLevel        = flag.String("log-level", "info", "log level. panic|fatal|error|warning|info|debug")

	version = "(none)"
)

type Server struct {
	partitioner partitioner.Partitioner
	store       mdata.Store
	index       idx.MetricIndex
	HTTPServer  *http.Server
}

func init() {
	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)
}

func main() {
	flag.Parse()

	// Only try and parse the conf file if it exists
	path := ""
	if _, err := os.Stat(*confFile); err == nil {
		path = *confFile
	}
	config, err := globalconf.NewWithOptions(&globalconf.Options{
		Filename:  path,
		EnvPrefix: "MT_",
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: configuration file error: %s", err)
		os.Exit(1)
	}

	mdata.ConfigSetup()
	cassandra.ConfigSetup()
	cassandraStore.ConfigSetup()
	bigtable.ConfigSetup()
	bigTableStore.ConfigSetup()

	config.ParseAll()

	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	lvl, err := log.ParseLevel(*logLevel)
	if err != nil {
		log.Fatalf("failed to parse log-level, %s", err.Error())
	}
	log.SetLevel(lvl)
	log.Infof("logging level set to '%s'", *logLevel)

	// the specified port is not relevant as we don't use clustering with this tool
	cluster.Init("mt-whisper-importer-writer", version, time.Now(), "http", int(80))

	mdata.ConfigProcess()
	cassandra.ConfigProcess()
	bigtable.ConfigProcess()
	bigTableStore.ConfigProcess(mdata.MaxChunkSpan())

	splits := strings.Split(*ttlsStr, ",")
	ttls := make([]uint32, 0)
	for _, split := range splits {
		ttls = append(ttls, dur.MustParseNDuration("ttl", split))
	}

	if (cassandraStore.CliConfig.Enabled && bigTableStore.CliConfig.Enabled) || !(cassandraStore.CliConfig.Enabled || bigTableStore.CliConfig.Enabled) {
		log.Fatalf("exactly 1 backend store plugin must be enabled. cassandra: %t bigtable: %t", cassandraStore.CliConfig.Enabled, bigTableStore.CliConfig.Enabled)
	}
	if (cassandra.CliConfig.Enabled && bigtable.CliConfig.Enabled) || !(cassandra.CliConfig.Enabled || bigtable.CliConfig.Enabled) {
		log.Fatalf("exactly 1 backend index plugin must be enabled. cassandra: %t bigtable: %t", cassandra.CliConfig.Enabled, bigtable.CliConfig.Enabled)
	}

	var index idx.MetricIndex
	if cassandra.CliConfig.Enabled {
		index = cassandra.New(cassandra.CliConfig)
	}
	if bigtable.CliConfig.Enabled {
		index = bigtable.New(bigtable.CliConfig)
	}

	var store mdata.Store
	if cassandraStore.CliConfig.Enabled {
		store, err = cassandraStore.NewCassandraStore(cassandraStore.CliConfig, mdata.TTLs())
		if err != nil {
			log.Fatalf("failed to initialize cassandra backend store. %s", err)
		}
	}
	if bigTableStore.CliConfig.Enabled {
		schemaMaxChunkSpan := mdata.MaxChunkSpan()
		store, err = bigTableStore.NewStore(bigTableStore.CliConfig, mdata.TTLs(), schemaMaxChunkSpan)
		if err != nil {
			log.Fatalf("failed to initialize bigtable backend store. %s", err)
		}
	}

	p, err := partitioner.NewKafka(*partitionScheme)
	if err != nil {
		panic(fmt.Sprintf("Failed to instantiate partitioner: %q", err))
	}

	index.Init()

	server := &Server{
		partitioner: p,
		index:       index,
		store:       store,
		HTTPServer: &http.Server{
			Addr:        *httpEndpoint,
			ReadTimeout: 10 * time.Minute,
		},
	}

	http.HandleFunc(*uriPath, server.chunksHandler)
	http.HandleFunc("/healthz", server.healthzHandler)

	log.Infof("Listening on %q", *httpEndpoint)
	err = http.ListenAndServe(*httpEndpoint, nil)
	if err != nil {
		panic(fmt.Sprintf("Error creating listener: %q", err))
	}
}

func throwError(w http.ResponseWriter, msg string) {
	w.WriteHeader(http.StatusBadRequest)
	w.Write([]byte(msg))
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
	data := mdata.ArchiveRequest{}
	err := data.UnmarshalCompressed(req.Body)
	if err != nil {
		throwError(w, fmt.Sprintf("Error decoding cwr stream: %q", err))
		return
	}

	if len(data.ChunkWriteRequests) == 0 {
		log.Warn("Received empty list of cwrs")
		return
	}

	log.Debugf(
		"Received %d cwrs for metric %s. The first has Key: %s, T0: %d, TTL: %d. The last has Key: %s, T0: %d, TTL: %d",
		len(data.ChunkWriteRequests),
		data.MetricData.Name,
		data.ChunkWriteRequests[0].Key.String(),
		data.ChunkWriteRequests[0].T0,
		data.ChunkWriteRequests[0].TTL,
		data.ChunkWriteRequests[len(data.ChunkWriteRequests)-1].Key.String(),
		data.ChunkWriteRequests[len(data.ChunkWriteRequests)-1].T0,
		data.ChunkWriteRequests[len(data.ChunkWriteRequests)-1].TTL)

	partition, err := s.partitioner.Partition(&data.MetricData, int32(*numPartitions))
	if err != nil {
		throwError(w, fmt.Sprintf("Error partitioning: %q", err))
		return
	}

	mkey, err := schema.MKeyFromString(data.MetricData.Id)
	if err != nil {
		throwError(w, fmt.Sprintf("Received invalid id: %s", data.MetricData.Id))
		return
	}

	s.index.AddOrUpdate(mkey, &data.MetricData, partition)
	for _, cwr := range data.ChunkWriteRequests {
		cwr := cwr // important because we pass by reference and this var will get overwritten in the next loop
		s.store.Add(&cwr)
	}
}
