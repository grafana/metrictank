package main

import (
	"flag"
	"fmt"
	l "log"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	_ "net/http/pprof"

	"github.com/Dieterbe/profiletrigger/heap"
	"github.com/Shopify/sarama"
	"github.com/benbjohnson/clock"
	"github.com/raintank/dur"
	"github.com/raintank/met"
	"github.com/raintank/met/helper"
	"github.com/raintank/metrictank/api"
	"github.com/raintank/metrictank/cluster"
	"github.com/raintank/metrictank/idx"
	"github.com/raintank/metrictank/idx/cassandra"
	"github.com/raintank/metrictank/idx/elasticsearch"
	"github.com/raintank/metrictank/idx/memory"
	"github.com/raintank/metrictank/input"
	inCarbon "github.com/raintank/metrictank/input/carbon"
	inKafkaMdm "github.com/raintank/metrictank/input/kafkamdm"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/mdata/chunk"
	"github.com/raintank/metrictank/mdata/notifierKafka"
	"github.com/raintank/metrictank/mdata/notifierNsq"
	"github.com/raintank/metrictank/usage"
	"github.com/raintank/metrictank/util"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"
)

var (
	logLevel     int
	warmupPeriod time.Duration
	startupTime  time.Time
	GitHash      = "(none)"

	metrics     *mdata.AggMetrics
	metricIndex idx.MetricIndex

	// Misc:
	instance    = flag.String("instance", "default", "instance identifier. must be unique. used in clustering messages, for naming queue consumers and emitted metrics")
	showVersion = flag.Bool("version", false, "print version string")
	confFile    = flag.String("config", "/etc/raintank/metrictank.ini", "configuration file path")

	accountingPeriodStr = flag.String("accounting-period", "5min", "accounting period to track per-org usage metrics")

	// Data:
	chunkSpanStr = flag.String("chunkspan", "2h", "duration of raw chunks")
	numChunksInt = flag.Int("numchunks", 5, "number of raw chunks to keep in memory. should be at least 1 more than what's needed to satisfy aggregation rules")
	ttlStr       = flag.String("ttl", "35d", "minimum wait before metrics are removed from storage")

	chunkMaxStaleStr  = flag.String("chunk-max-stale", "1h", "max age for a chunk before to be considered stale and to be persisted to Cassandra.")
	metricMaxStaleStr = flag.String("metric-max-stale", "6h", "max age for a metric before to be considered stale and to be purged from memory.")
	gcIntervalStr     = flag.String("gc-interval", "1h", "Interval to run garbage collection job.")
	warmUpPeriodStr   = flag.String("warm-up-period", "1h", "duration before secondary nodes start serving requests")

	aggSettings = flag.String("agg-settings", "", "aggregation settings: <agg span in seconds>:<agg chunkspan in seconds>:<agg numchunks>:<ttl in seconds>[:<ready as bool. default true>] (may be given multiple times as comma-separated list)")

	// Cassandra:
	cassandraAddrs               = flag.String("cassandra-addrs", "localhost", "cassandra host (may be given multiple times as comma-separated list)")
	cassandraKeyspace            = flag.String("cassandra-keyspace", "raintank", "cassandra keyspace to use for storing the metric data table")
	cassandraConsistency         = flag.String("cassandra-consistency", "one", "write consistency (any|one|two|three|quorum|all|local_quorum|each_quorum|local_one")
	cassandraHostSelectionPolicy = flag.String("cassandra-host-selection-policy", "roundrobin", "")
	cassandraTimeout             = flag.Int("cassandra-timeout", 1000, "cassandra timeout in milliseconds")
	cassandraReadConcurrency     = flag.Int("cassandra-read-concurrency", 20, "max number of concurrent reads to cassandra.")
	cassandraWriteConcurrency    = flag.Int("cassandra-write-concurrency", 10, "max number of concurrent writes to cassandra.")
	cassandraReadQueueSize       = flag.Int("cassandra-read-queue-size", 100, "max number of outstanding reads before blocking. value doesn't matter much")
	cassandraWriteQueueSize      = flag.Int("cassandra-write-queue-size", 100000, "write queue size per cassandra worker. should be large engough to hold all at least the total number of series expected, divided by how many workers you have")
	cassandraRetries             = flag.Int("cassandra-retries", 0, "how many times to retry a query before failing it")
	cqlProtocolVersion           = flag.Int("cql-protocol-version", 4, "cql protocol version to use")

	cassandraSSL              = flag.Bool("cassandra-ssl", false, "enable SSL connection to cassandra")
	cassandraCaPath           = flag.String("cassandra-ca-path", "/etc/raintank/ca.pem", "cassandra CA certificate path when using SSL")
	cassandraHostVerification = flag.Bool("cassandra-host-verification", true, "host (hostname and server cert) verification when using SSL")

	cassandraAuth     = flag.Bool("cassandra-auth", false, "enable cassandra authentication")
	cassandraUsername = flag.String("cassandra-username", "cassandra", "username for authentication")
	cassandraPassword = flag.String("cassandra-password", "cassandra", "password for authentication")

	// Profiling, instrumentation and logging:
	blockProfileRate = flag.Int("block-profile-rate", 0, "see https://golang.org/pkg/runtime/#SetBlockProfileRate")
	memProfileRate   = flag.Int("mem-profile-rate", 512*1024, "0 to disable. 1 for max precision (expensive!) see https://golang.org/pkg/runtime/#pkg-variables")

	statsdEnabled = flag.Bool("statsd-enabled", true, "enable sending statsd messages for instrumentation")
	statsdAddr    = flag.String("statsd-addr", "localhost:8125", "statsd address")
	statsdType    = flag.String("statsd-type", "standard", "statsd type: standard or datadog")

	proftrigPath       = flag.String("proftrigger-path", "/tmp", "path to store triggered profiles")
	proftrigFreqStr    = flag.String("proftrigger-freq", "60s", "inspect status frequency. set to 0 to disable")
	proftrigMinDiffStr = flag.String("proftrigger-min-diff", "1h", "minimum time between triggered profiles")
	proftrigHeapThresh = flag.Int("proftrigger-heap-thresh", 25000000000, "if this many bytes allocated, trigger a profile")

	cassWriteQueueSize met.Gauge
	cassWriters        met.Gauge
	points             met.Gauge

	// metric bytes_alloc.not_freed is a gauge of currently allocated (within the runtime) memory.
	// it does not include freed data so it drops at every GC run.
	alloc met.Gauge
	// metric bytes_alloc.incl_freed is a counter of total amount of bytes allocated during process lifetime. (incl freed data)
	totalAlloc met.Gauge
	// metric bytes_sys is the amount of bytes currently obtained from the system by the process.  This is what the profiletrigger looks at.
	sysBytes       met.Gauge
	clusterPrimary met.Gauge

	// metric cluster.promotion_wait is how long a candidate (secondary node) has to wait until it can become a primary
	// When the timer becomes 0 it means the in-memory buffer has been able to fully populate so that if you stop a primary
	// and it was able to save its complete chunks, this node will be able to take over without dataloss.
	// You can upgrade a candidate to primary while the timer is not 0 yet, it just means it may have missing data in the chunks that it will save.
	clusterPromoWait met.Gauge
	gcNum            met.Gauge // go GC
	gcDur            met.Gauge // go GC
	gcCpuFraction    met.Gauge // go GC

	// metric gc.heap_objects is how many objects are allocated on the heap, it's a key indicator for GC workload
	heapObjects met.Gauge

	promotionReadyAtChan chan uint32
)

func init() {
	flag.IntVar(&logLevel, "log-level", 2, "log level. 0=TRACE|1=DEBUG|2=INFO|3=WARN|4=ERROR|5=CRITICAL|6=FATAL")
}

func main() {
	startupTime = time.Now()

	/***********************************
		Initialize Configuration
	***********************************/
	flag.Parse()

	// if the user just wants the version, give it and exit
	if *showVersion {
		fmt.Printf("metrictank (built with %s, git hash %s)\n", runtime.Version(), GitHash)
		return
	}

	// Only try and parse the conf file if it exists
	path := ""
	if _, err := os.Stat(*confFile); err == nil {
		path = *confFile
	}
	conf, err := globalconf.NewWithOptions(&globalconf.Options{
		Filename:  path,
		EnvPrefix: "MT_",
	})
	if err != nil {
		log.Fatal(4, "error with configuration file: %s", err)
		os.Exit(1)
	}
	// load config for metric ingestors
	inCarbon.ConfigSetup()
	inKafkaMdm.ConfigSetup()

	// load config for cluster handlers
	notifierNsq.ConfigSetup()
	notifierKafka.ConfigSetup()

	// load config for metricIndexers
	memory.ConfigSetup()
	elasticsearch.ConfigSetup()
	cassandra.ConfigSetup()

	// load config for API
	api.ConfigSetup()

	// load config for cluster
	cluster.ConfigSetup()

	conf.ParseAll()

	/***********************************
		Initialize Logging
	***********************************/
	log.NewLogger(0, "console", fmt.Sprintf(`{"level": %d, "formatting":false}`, logLevel))
	mdata.LogLevel = logLevel
	inKafkaMdm.LogLevel = logLevel
	api.LogLevel = logLevel
	// workaround for https://github.com/grafana/grafana/issues/4055
	switch logLevel {
	case 0:
		log.Level(log.TRACE)
	case 1:
		log.Level(log.DEBUG)
	case 2:
		log.Level(log.INFO)
	case 3:
		log.Level(log.WARN)
	case 4:
		log.Level(log.ERROR)
	case 5:
		log.Level(log.CRITICAL)
	case 6:
		log.Level(log.FATAL)
	}

	/***********************************
		Validate  settings needed for clustering
	***********************************/
	if *instance == "" {
		log.Fatal(4, "instance can't be empty")
	}

	log.Info("Metrictank starting. Built from %s - Go version %s", GitHash, runtime.Version())

	/***********************************
		Initialize our Cluster
	***********************************/
	cluster.Init(*instance, GitHash, startupTime)

	/***********************************
		Validate remaining settings
	***********************************/
	inCarbon.ConfigProcess()
	inKafkaMdm.ConfigProcess(*instance)
	notifierNsq.ConfigProcess()
	notifierKafka.ConfigProcess(*instance)
	api.ConfigProcess()
	cluster.ConfigProcess()

	if !inCarbon.Enabled && !inKafkaMdm.Enabled {
		log.Fatal(4, "you should enable at least 1 input plugin")
	}

	sec := dur.MustParseUNsec("warm-up-period", *warmUpPeriodStr)
	warmupPeriod = time.Duration(sec) * time.Second

	promotionReadyAtChan = make(chan uint32)

	chunkSpan := dur.MustParseUNsec("chunkspan", *chunkSpanStr)
	numChunks := uint32(*numChunksInt)
	chunkMaxStale := dur.MustParseUNsec("chunk-max-stale", *chunkMaxStaleStr)
	metricMaxStale := dur.MustParseUNsec("metric-max-stale", *metricMaxStaleStr)
	gcInterval := time.Duration(dur.MustParseUNsec("gc-interval", *gcIntervalStr)) * time.Second
	ttl := dur.MustParseUNsec("ttl", *ttlStr)
	if (mdata.Month_sec % chunkSpan) != 0 {
		log.Fatal(4, "chunkSpan must fit without remainders into month_sec (28*24*60*60)")
	}
	_, ok := chunk.RevChunkSpans[chunkSpan]
	if !ok {
		log.Fatal(4, "chunkSpan %s is not a valid value (https://github.com/raintank/metrictank/blob/master/docs/data-knobs.md#valid-chunk-spans)", *chunkSpanStr)
	}

	set := strings.Split(*aggSettings, ",")
	finalSettings := make([]mdata.AggSetting, 0)
	highestChunkSpan := chunkSpan
	for _, v := range set {
		if v == "" {
			continue
		}
		fields := strings.Split(v, ":")
		if len(fields) < 4 {
			log.Fatal(4, "bad agg settings")
		}
		aggSpan := dur.MustParseUNsec("aggsettings", fields[0])
		aggChunkSpan := dur.MustParseUNsec("aggsettings", fields[1])
		aggNumChunks := dur.MustParseUNsec("aggsettings", fields[2])
		aggTTL := dur.MustParseUNsec("aggsettings", fields[3])
		if (mdata.Month_sec % aggChunkSpan) != 0 {
			log.Fatal(4, "aggChunkSpan must fit without remainders into month_sec (28*24*60*60)")
		}
		_, ok := chunk.RevChunkSpans[aggChunkSpan]
		if !ok {
			log.Fatal(4, "aggChunkSpan %s is not a valid value (https://github.com/raintank/metrictank/blob/master/docs/data-knobs.md#valid-chunk-spans)", fields[1])
		}
		highestChunkSpan = util.Max(highestChunkSpan, aggChunkSpan)
		ready := true
		if len(fields) == 5 {
			ready, err = strconv.ParseBool(fields[4])
			if err != nil {
				log.Fatal(4, "aggsettings ready: %s", err)
			}
		}
		finalSettings = append(finalSettings, mdata.NewAggSetting(aggSpan, aggChunkSpan, aggNumChunks, aggTTL, ready))
	}
	proftrigFreq := dur.MustParseUsec("proftrigger-freq", *proftrigFreqStr)
	proftrigMinDiff := int(dur.MustParseUNsec("proftrigger-min-diff", *proftrigMinDiffStr))
	if proftrigFreq > 0 {
		errors := make(chan error)
		trigger, _ := heap.New(*proftrigPath, *proftrigHeapThresh, proftrigMinDiff, time.Duration(proftrigFreq)*time.Second, errors)
		go func() {
			for e := range errors {
				log.Error(0, "profiletrigger heap: %s", e)
			}
		}()
		go trigger.Run()
	}

	accountingPeriod := dur.MustParseUNsec("accounting-period", *accountingPeriodStr)

	/***********************************
		configure Profiling
	***********************************/
	runtime.SetBlockProfileRate(*blockProfileRate)
	runtime.MemProfileRate = *memProfileRate

	/************************************
	    handle interupt signals
	************************************/
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	/***********************************
		configure StatsD
	***********************************/
	if !*statsdEnabled {
		log.Warn("running metrictank without statsd instrumentation.")
	}
	stats, err := helper.New(*statsdEnabled, *statsdAddr, *statsdType, "metrictank", *instance)
	if err != nil {
		log.Fatal(4, "failed to initialize statsd. %s", err)
	}
	initMetrics(stats)

	/*************************************
	  Start polling our Cluster Peers
	**************************************/
	cluster.Start()

	/***********************************
		Initialize our backendStore
	***********************************/
	store, err := mdata.NewCassandraStore(stats, *cassandraAddrs, *cassandraKeyspace, *cassandraConsistency, *cassandraCaPath, *cassandraUsername, *cassandraPassword, *cassandraHostSelectionPolicy, *cassandraTimeout, *cassandraReadConcurrency, *cassandraWriteConcurrency, *cassandraReadQueueSize, *cassandraWriteQueueSize, *cassandraRetries, *cqlProtocolVersion, *cassandraSSL, *cassandraAuth, *cassandraHostVerification)
	if err != nil {
		log.Fatal(4, "failed to initialize cassandra. %s", err)
	}
	store.InitMetrics(stats)

	/***********************************
		Initialize our MemoryStore
	***********************************/
	metrics = mdata.NewAggMetrics(store, chunkSpan, numChunks, chunkMaxStale, metricMaxStale, ttl, gcInterval, finalSettings)
	mdata.InitMetrics(stats)

	/***********************************
		Initialize our Inputs
	***********************************/
	inputs := make([]input.Plugin, 0)
	// note. all these New functions must either return a valid instance or call log.Fatal
	if inCarbon.Enabled {
		inputs = append(inputs, inCarbon.New(stats))
	}

	if inKafkaMdm.Enabled {
		sarama.Logger = l.New(os.Stdout, "[Sarama] ", l.LstdFlags)
		inputs = append(inputs, inKafkaMdm.New(stats))
	}

	if cluster.Mode == cluster.ModeMulti && len(inputs) > 1 {
		log.Warn("It is not recommended to run a mulitnode cluster with more then 1 input plugin.")
	}

	/***********************************
		Initialize MetricPerrist notifiers
	***********************************/
	handlers := make([]mdata.NotifierHandler, 0)
	if notifierKafka.Enabled {
		handlers = append(handlers, notifierKafka.New(*instance, metrics, stats))
	}

	if notifierNsq.Enabled {
		handlers = append(handlers, notifierNsq.New(*instance, metrics, stats))
	}

	mdata.InitPersistNotifier(stats, handlers...)

	/***********************************
		Initialize our MetricIdx
	***********************************/
	pre := time.Now()

	if memory.Enabled {
		if metricIndex != nil {
			log.Fatal(4, "Only 1 metricIndex handler can be enabled.")
		}
		metricIndex = memory.New()
	}
	if elasticsearch.Enabled {
		if metricIndex != nil {
			log.Fatal(4, "Only 1 metricIndex handler can be enabled.")
		}
		metricIndex = elasticsearch.New()
	}
	if cassandra.Enabled {
		if metricIndex != nil {
			log.Fatal(4, "Only 1 metricIndex handler can be enabled.")
		}
		metricIndex = cassandra.New()
	}

	if metricIndex == nil {
		log.Fatal(4, "No metricIndex handlers enabled.")
	}

	err = metricIndex.Init(stats)
	if err != nil {
		log.Fatal(4, "failed to initialize metricIndex: %s", err)
	}
	log.Info("metricIndex initialized in %s. starting data consumption", time.Now().Sub(pre))

	/***********************************
		Initialize usage Reporting
	***********************************/
	usg := usage.New(accountingPeriod, metrics, metricIndex, clock.New())

	/***********************************
		Start our inputs
	***********************************/
	for _, plugin := range inputs {
		plugin.Start(metrics, metricIndex, usg)
	}

	promotionReadyAtChan <- (uint32(time.Now().Unix())/highestChunkSpan + 1) * highestChunkSpan

	/***********************************
		Initialize our API server
	***********************************/
	apiServer, err := api.NewServer(stats)
	if err != nil {
		log.Fatal(4, "Failed to start API. %s", err.Error())
	}

	apiServer.BindMetricIndex(metricIndex)
	apiServer.BindMemoryStore(metrics)
	apiServer.BindBackendStore(store)
	go apiServer.Run()

	/***********************************
		Set our status so we can accept
		requests from users.
	***********************************/
	if cluster.ThisNode.IsPrimary() {
		cluster.ThisNode.SetReady()
	} else {
		cluster.ThisNode.SetReadyIn(warmupPeriod)
	}

	/***********************************
		Wait for Shutdown
	***********************************/
	<-sigChan

	// stop API
	apiServer.Stop()
	// stop polling peer nodes
	cluster.Stop()

	// shutdown our input plugins.  These may take a while as we allow them
	// to finish processing any metrics that have already been ingested.
	timer := time.NewTimer(time.Second * 10)
	var wg sync.WaitGroup
	for _, plugin := range inputs {
		wg.Add(1)
		go func(plugin input.Plugin) {
			log.Info("Shutting down %s consumer", plugin.Name())
			plugin.Stop()
			log.Info("%s consumer finished shutdown", plugin.Name())
			wg.Done()
		}(plugin)
	}
	pluginsStopped := make(chan struct{})
	go func() {
		wg.Wait()
		close(pluginsStopped)
	}()
	select {
	case <-timer.C:
		log.Warn("Plugins taking too long to shutdown, not waiting any longer.")
	case <-pluginsStopped:
		timer.Stop()
	}

	log.Info("closing store")
	store.Stop()
	metricIndex.Stop()
	log.Info("terminating.")
	log.Close()

}

func initMetrics(stats met.Backend) {
	cassWriteQueueSize = stats.NewGauge("cassandra.write_queue.size", int64(*cassandraWriteQueueSize))
	cassWriters = stats.NewGauge("cassandra.num_writers", int64(*cassandraWriteConcurrency))
	points = stats.NewGauge("total_points", 0)
	alloc = stats.NewGauge("bytes_alloc.not_freed", 0)
	totalAlloc = stats.NewGauge("bytes_alloc.incl_freed", 0)
	sysBytes = stats.NewGauge("bytes_sys", 0)
	clusterPrimary = stats.NewGauge("cluster.primary", 0)
	clusterPromoWait = stats.NewGauge("cluster.promotion_wait", 1)
	gcNum = stats.NewGauge("gc.num", 0)
	gcDur = stats.NewGauge("gc.dur", 0)                 // in nanoseconds. last known duration.
	gcCpuFraction = stats.NewGauge("gc.cpufraction", 0) // reported as pro-mille
	heapObjects = stats.NewGauge("gc.heap_objects", 0)

	// run a collector for some global stats
	go func() {
		var m runtime.MemStats
		var promotionReadyAtTs uint32

		ticker := time.Tick(time.Duration(1) * time.Second)
		for {
			select {
			case now := <-ticker:
				points.Value(int64(chunk.TotalPoints()))
				runtime.ReadMemStats(&m)
				alloc.Value(int64(m.Alloc))
				totalAlloc.Value(int64(m.TotalAlloc))
				sysBytes.Value(int64(m.Sys))
				gcNum.Value(int64(m.NumGC))
				gcDur.Value(int64(m.PauseNs[(m.NumGC+255)%256]))
				gcCpuFraction.Value(int64(1000 * m.GCCPUFraction))
				heapObjects.Value(int64(m.HeapObjects))
				var px int64
				if cluster.ThisNode.IsPrimary() {
					px = 1
				} else {
					px = 0
				}
				clusterPrimary.Value(px)
				cassWriters.Value(int64(*cassandraWriteConcurrency))
				cassWriteQueueSize.Value(int64(*cassandraWriteQueueSize))
				unix := uint32(now.Unix())
				if unix >= promotionReadyAtTs {
					if promotionReadyAtTs == 0 {
						// not set yet. operator should hold off
						clusterPromoWait.Value(1)
					} else {
						clusterPromoWait.Value(0)
					}
				} else {
					clusterPromoWait.Value(int64(promotionReadyAtTs - unix))
				}
			case promotionReadyAtTs = <-promotionReadyAtChan:
			}
		}
	}()
}
