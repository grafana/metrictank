package main

import (
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/grafana/globalconf"
	"github.com/grafana/metrictank/schema"
	log "github.com/sirupsen/logrus"

	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/cluster/partitioner"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/idx/bigtable"
	"github.com/grafana/metrictank/idx/cassandra"
	"github.com/grafana/metrictank/logger"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/mdata/importer"
	bigTableStore "github.com/grafana/metrictank/store/bigtable"
	cassandraStore "github.com/grafana/metrictank/store/cassandra"
)

var (
	confFile        = flag.String("config", "/etc/metrictank/metrictank.ini", "configuration file path")
	exitOnError     = flag.Bool("exit-on-error", false, "Exit with a message when there's an error")
	httpEndpoint    = flag.String("http-endpoint", "0.0.0.0:8080", "The http endpoint to listen on")
	partitionScheme = flag.String("partition-scheme", "bySeries", "method used for partitioning metrics. This should match the settings of tsdb-gw. (byOrg|bySeries|bySeriesWithTags|bySeriesWithTagsFnv)")
	uriPath         = flag.String("uri-path", "/metrics/import", "the URI on which we expect chunks to get posted")
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

	if *numPartitions < 1 {
		log.Fatalf("number of partitions must be set to at least 1")
	}

	// the specified port is not relevant as we don't use clustering with this tool
	cluster.Init("mt-whisper-importer-writer", version, time.Now(), "http", int(80))

	mdata.ConfigProcess()
	cassandra.ConfigProcess()
	bigtable.ConfigProcess()
	bigTableStore.ConfigProcess(mdata.MaxChunkSpan())

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
		store, err = cassandraStore.NewCassandraStore(cassandraStore.CliConfig, mdata.TTLs(), 86400)
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
	orgId, err := getOrgId(req)
	if err != nil {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(err.Error()))
		return
	}

	data := importer.ArchiveRequest{}
	err = data.UnmarshalCompressed(req.Body)
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
		data.ChunkWriteRequests[0].Archive.String(),
		data.ChunkWriteRequests[0].T0,
		data.ChunkWriteRequests[0].TTL,
		data.ChunkWriteRequests[len(data.ChunkWriteRequests)-1].Archive.String(),
		data.ChunkWriteRequests[len(data.ChunkWriteRequests)-1].T0,
		data.ChunkWriteRequests[len(data.ChunkWriteRequests)-1].TTL)

	data.MetricData.OrgId = orgId
	data.MetricData.SetId()
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

	var wg sync.WaitGroup
	for _, cwr := range data.ChunkWriteRequests {
		wg.Add(1)
		cb := func() {
			wg.Done()
			log.Debugf("Cwr has been written: %s / %s / %d", data.MetricData.Name, cwr.Archive.String(), cwr.T0)
		}
		cwrWithOrg := cwr.GetChunkWriteRequest(cb, mkey)
		s.store.Add(&cwrWithOrg)
	}

	wg.Wait()
	log.Infof("Successfully wrote %d cwrs for metric %s", len(data.ChunkWriteRequests), data.MetricData.Name)
}

func getOrgId(req *http.Request) (int, error) {
	if orgIdStr := req.Header.Get("X-Org-Id"); len(orgIdStr) > 0 {
		if orgId, err := strconv.Atoi(orgIdStr); err == nil {
			return orgId, nil
		} else {
			return 0, fmt.Errorf("Invalid value in X-Org-Id header (%s): %s", orgIdStr, err)
		}
	}
	return 0, errors.New("Missing X-Org-Id header")
}
