package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/grafana/grafana/pkg/log"
	"github.com/nsqio/go-nsq"
	"github.com/raintank/met"
	"github.com/raintank/met/helper"
	"github.com/raintank/raintank-metric/app"
	"github.com/raintank/raintank-metric/instrumented_nsq"
	"github.com/raintank/raintank-metric/metricdef"
	"github.com/rakyll/globalconf"
)

var (
	showVersion = flag.Bool("version", false, "print version string")
	primaryNode = flag.Bool("primary-node", false, "the primary node writes data to cassnadra. There should only be 1 primary node per cluster of nodes.")

	concurrency        = flag.Int("concurrency", 10, "number of workers parsing messages")
	topic              = flag.String("topic", "metrics", "NSQ topic")
	topicNotifyPersist = flag.String("topic-notify-persist", "metricpersist", "NSQ topic")
	channel            = flag.String("channel", "tank", "NSQ channel for both metric topic and metric-persist topic")
	instance           = flag.String("instance", "default", "cluster node name and value used to differentiate metrics between nodes")
	maxInFlight        = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")
	chunkSpan          = flag.Int("chunkspan", 120, "chunk span in seconds")
	numChunks          = flag.Int("numchunks", 5, "number of chunks to keep in memory. should be at least 1 more than what's needed to satisfy aggregation rules")
	warmUpPeriod       = flag.Int("warm-up-period", 3600, "number of seconds before secondary nodes start serving requests")

	cassandraWriteConcurrency = flag.Int("cassandra-write-concurrency", 10, "max number of concurrent writes to cassandra.")
	cassandraWriteQueueSize   = flag.Int("cassandra-write-queue-size", 100000, "write queue size per cassandra worker. should be large engough to hold all at least the total number of series expected, divided by how many workers you have")
	cassandraPort             = flag.Int("cassandra-port", 9042, "cassandra port")
	cassandraAddrs            = flag.String("cassandra-addrs", "", "cassandra host (may be given multiple times as comma-separated list)")
	metricTTL                 = flag.Int("ttl", 3024000, "seconds before metrics are removed from cassandra")

	listenAddr = flag.String("listen", ":6060", "http listener address.")
	esAddr     = flag.String("elastic-addr", "localhost:9200", "elasticsearch address for metric definitions")
	indexName  = flag.String("index-name", "metric", "Elasticsearch index name for storing metric index.")

	statsdAddr = flag.String("statsd-addr", "localhost:8125", "statsd address")
	statsdType = flag.String("statsd-type", "standard", "statsd type: standard or datadog")

	gcInterval     = flag.Int("gc-interval", 3600, "Interval in seconds to run garbage collection job.")
	chunkMaxStale  = flag.Int("chunk-max-stale", 3600, "max age in seconds for a chunk before to be considered stale and to be persisted to Cassandra.")
	metricMaxStale = flag.Int("metric-max-stale", 21600, "max age in seconds for a metric before to be considered stale and to be purged from memory.")

	logLevel = flag.Int("log-level", 2, "log level. 0=TRACE|1=DEBUG|2=INFO|3=WARN|4=ERROR|5=CRITICAL|6=FATAL")

	confFile = flag.String("config", "/etc/raintank/metric_tank.ini", "configuration file path")

	producerOpts     = flag.String("producer-opt", "", "option to passthrough to nsq.Producer (may be given multiple times as comma-separated list, see http://godoc.org/github.com/nsqio/go-nsq#Config)")
	consumerOpts     = flag.String("consumer-opt", "", "option to passthrough to nsq.Consumer (may be given multiple times as comma-separated list, http://godoc.org/github.com/nsqio/go-nsq#Config)")
	nsqdTCPAddrs     = flag.String("nsqd-tcp-address", "", "nsqd TCP address (may be given multiple times as comma-separated list)")
	lookupdHTTPAddrs = flag.String("lookupd-http-address", "", "lookupd HTTP address (may be given multiple times as comma-separated list)")
	aggSettings      = flag.String("agg-settings", "", "aggregation settings: <agg span in seconds>:<agg chunkspan in seconds>:<agg numchunks>:<ttl in seconds> (may be given multiple times as comma-separated list)")

	metrics  *AggMetrics
	defCache *DefCache

	startupTime time.Time
)

var reqSpanMem met.Meter
var reqSpanBoth met.Meter

// it's pretty expensive/impossible to do chunk sive in mem vs in cassandra etc, but we can more easily measure chunk sizes when we operate on them
var chunkSizeAtSave met.Meter
var chunkSizeAtLoad met.Meter
var chunkCreate met.Count
var chunkClear met.Count
var chunkSaveOk met.Count
var chunkSaveFail met.Count
var metricDefCacheHit met.Count
var metricDefCacheMiss met.Count
var metricsReceived met.Count
var metricsToCassandraOK met.Count
var metricsToCassandraFail met.Count
var cassandraRowsPerResponse met.Meter
var cassandraChunksPerRow met.Meter
var messagesSize met.Meter
var metricsPerMessage met.Meter
var msgsAge met.Meter // in ms
// just 1 global timer of request handling time. includes mem/cassandra gets, chunk decode/iters, json building etc
// there is such a thing as too many metrics.  we have this, and cassandra timings, that should be enough for realtime profiling
var reqHandleDuration met.Timer
var cassandraPutDuration met.Timer
var cassandraBlockDuration met.Timer
var cassandraGetDuration met.Timer
var inItems met.Meter
var points met.Gauge
var msgsHandleOK met.Count
var msgsHandleFail met.Count
var alloc met.Gauge
var totalAlloc met.Gauge
var sysBytes met.Gauge
var metricsActive met.Gauge
var metricsToEsOK met.Count
var metricsToEsFail met.Count
var esPutDuration met.Timer // note that due to our use of bulk indexer, most values will be very fast with the occasional "outlier" which triggers a flush
var clusterPrimary met.Gauge
var gcNum met.Gauge
var gcDur met.Gauge

func main() {
	startupTime = time.Now()
	flag.Parse()

	// Only try and parse the conf file if it exists
	if _, err := os.Stat(*confFile); err == nil {
		conf, err := globalconf.NewWithOptions(&globalconf.Options{Filename: *confFile})
		if err != nil {
			log.Fatal(4, "error with configuration file: %s", err)
			os.Exit(1)
		}
		conf.ParseAll()
	}

	log.NewLogger(0, "console", fmt.Sprintf(`{"level": %d, "formatting":true}`, *logLevel))
	// workaround for https://github.com/grafana/grafana/issues/4055
	switch *logLevel {
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

	if *showVersion {
		fmt.Println("metrics_tank")
		return
	}
	if *instance == "" {
		log.Fatal(4, "instance can't be empty")
	}
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(4, "failed to lookup hostname. %s", err)
	}
	stats, err := helper.New(true, *statsdAddr, *statsdType, "metric_tank", strings.Replace(hostname, ".", "_", -1))
	if err != nil {
		log.Fatal(4, "failed to initialize statsd. %s", err)
	}

	if *channel == "" {
		rand.Seed(time.Now().UnixNano())
		*channel = fmt.Sprintf("metric_tank%06d#ephemeral", rand.Int()%999999)
	}

	if *topic == "" {
		log.Fatal(4, "--topic is required")
	}

	if *nsqdTCPAddrs == "" && *lookupdHTTPAddrs == "" {
		log.Fatal(4, "--nsqd-tcp-address or --lookupd-http-address required")
	}
	if *nsqdTCPAddrs != "" && *lookupdHTTPAddrs != "" {
		log.Fatal(4, "use --nsqd-tcp-address or --lookupd-http-address not both")
	}
	// set default cassandra address if none is set.
	if *cassandraAddrs == "" {
		*cassandraAddrs = "localhost"
	}

	defs, err := metricdef.NewDefsEs(*esAddr, "", "", *indexName)
	if err != nil {
		log.Fatal(4, "failed to initialize Elasticsearch. %s", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	initMetrics(stats)

	set := strings.Split(*aggSettings, ",")
	finalSettings := make([]aggSetting, 0)
	for _, v := range set {
		if v == "" {
			continue
		}
		fields := strings.Split(v, ":")
		if len(fields) != 4 {
			log.Fatal(4, "bad agg settings")
		}
		aggSpan, err := strconv.Atoi(fields[0])
		if err != nil {
			log.Fatal(4, "bad agg settings", err)
		}
		aggChunkSpan, err := strconv.Atoi(fields[1])
		if err != nil {
			log.Fatal(4, "bad agg settings", err)
		}
		if (month_sec % aggChunkSpan) != 0 {
			panic("aggChunkSpan must fit without remainders into month_sec (28*24*60*60)")
		}
		aggNumChunks, err := strconv.Atoi(fields[2])
		if err != nil {
			log.Fatal(0, "bad agg settings", err)
		}
		aggTTL, err := strconv.Atoi(fields[3])
		if err != nil {
			log.Fatal(0, "bad agg settings", err)
		}
		finalSettings = append(finalSettings, aggSetting{uint32(aggSpan), uint32(aggChunkSpan), uint32(aggNumChunks), uint32(aggTTL)})
	}
	if (month_sec % *chunkSpan) != 0 {
		panic("aggChunkSpan must fit without remainders into month_sec (28*24*60*60)")
	}

	store, err := NewCassandraStore()
	if err != nil {
		log.Fatal(4, "failed to initialize cassandra. %s", err)
	}

	// set our cluster state before we start consuming messages.
	clusterStatus = NewClusterStatus(*instance, *primaryNode)

	cfg := nsq.NewConfig()
	cfg.UserAgent = "metrics_tank"
	err = app.ParseOpts(cfg, *consumerOpts)
	if err != nil {
		log.Fatal(4, "failed to parse nsq consumer options. %s", err)
	}
	cfg.MaxInFlight = *maxInFlight

	consumer, err := insq.NewConsumer(*topic, *channel, cfg, "%s", stats)
	if err != nil {
		log.Fatal(4, "Failed to create NSQ consumer. %s", err)
	}

	metrics = NewAggMetrics(store, uint32(*chunkSpan), uint32(*numChunks), uint32(*chunkMaxStale), uint32(*metricMaxStale), uint32(*metricTTL), finalSettings)
	pre := time.Now()
	defCache = NewDefCache(defs)
	handler := NewHandler(metrics, defCache)
	log.Info("DefCache initialized in %s. starting data consumption", time.Now().Sub(pre))
	consumer.AddConcurrentHandlers(handler, *concurrency)

	nsqdAdds := strings.Split(*nsqdTCPAddrs, ",")
	if len(nsqdAdds) == 1 && nsqdAdds[0] == "" {
		nsqdAdds = []string{}
	}

	lookupdAdds := strings.Split(*lookupdHTTPAddrs, ",")
	if len(lookupdAdds) == 1 && lookupdAdds[0] == "" {
		lookupdAdds = []string{}
	}

	go func() {
		time.Sleep(100 * time.Millisecond)
		err = consumer.ConnectToNSQDs(nsqdAdds)
		if err != nil {
			log.Fatal(4, "failed to connect to NSQDs. %s", err)
		}
		log.Info("consumer connected to nsqd")

		err = consumer.ConnectToNSQLookupds(lookupdAdds)
		if err != nil {
			log.Fatal(4, "failed to connect to NSQLookupds. %s", err)
		}
		log.Info("consumer connected to nsqlookupd")
	}()

	InitCluster(metrics, stats)

	go func() {
		http.HandleFunc("/", appStatus)
		http.HandleFunc("/get", get(store, defCache, finalSettings))
		http.HandleFunc("/cluster", clusterStatusHandler)
		log.Info("starting listener for metrics and http/debug on %s", *listenAddr)
		log.Info("%s", http.ListenAndServe(*listenAddr, nil))
	}()

	for {
		select {
		case <-consumer.StopChan:
			log.Info("closing store")
			store.Stop()
			defs.Stop()
			log.Info("terminating.")
			log.Close()
			return
		case <-sigChan:
			log.Info("Shutting down")
			consumer.Stop()
		}
	}
}

func initMetrics(stats met.Backend) {
	reqSpanMem = stats.NewMeter("requests_span.mem", 0)
	reqSpanBoth = stats.NewMeter("requests_span.mem_and_cassandra", 0)
	chunkSizeAtSave = stats.NewMeter("chunk_size.at_save", 0)
	chunkSizeAtLoad = stats.NewMeter("chunk_size.at_load", 0)
	chunkCreate = stats.NewCount("chunks.create")
	chunkClear = stats.NewCount("chunks.clear")
	chunkSaveOk = stats.NewCount("chunks.save_ok")
	chunkSaveFail = stats.NewCount("chunks.save_fail")
	metricDefCacheHit = stats.NewCount("metricmeta_cache.hit")
	metricDefCacheMiss = stats.NewCount("metricmeta_cache.miss")
	metricsReceived = stats.NewCount("metrics_received")
	metricsToCassandraOK = stats.NewCount("metrics_to_cassandra.ok")
	metricsToCassandraFail = stats.NewCount("metrics_to_cassandra.fail")
	cassandraRowsPerResponse = stats.NewMeter("cassandra_rows_per_response", 0)
	cassandraChunksPerRow = stats.NewMeter("cassandra_chunks_per_row", 0)
	messagesSize = stats.NewMeter("message_size", 0)
	metricsPerMessage = stats.NewMeter("metrics_per_message", 0)
	msgsAge = stats.NewMeter("message_age", 0)
	reqHandleDuration = stats.NewTimer("request_handle_duration", 0)
	cassandraGetDuration = stats.NewTimer("cassandra_get_duration", 0)
	cassandraBlockDuration = stats.NewTimer("cassandra_block_duration", 0)
	cassandraPutDuration = stats.NewTimer("cassandra_put_duration", 0)
	inItems = stats.NewMeter("in.items", 0)
	points = stats.NewGauge("total_points", 0)
	msgsHandleOK = stats.NewCount("handle.ok")
	msgsHandleFail = stats.NewCount("handle.fail")
	alloc = stats.NewGauge("bytes_alloc.not_freed", 0)
	totalAlloc = stats.NewGauge("bytes_alloc.incl_freed", 0)
	sysBytes = stats.NewGauge("bytes_sys", 0)
	metricsActive = stats.NewGauge("metrics_active", 0)
	metricsToEsOK = stats.NewCount("metrics_to_es.ok")
	metricsToEsFail = stats.NewCount("metrics_to_es.fail")
	esPutDuration = stats.NewTimer("es_put_duration", 0)
	clusterPrimary = stats.NewGauge("cluster.primary", 0)
	gcNum = stats.NewGauge("gc.num", 0)
	gcDur = stats.NewGauge("gc.dur", 0) // in nanoseconds. last known duration.

	// run a collector for some global stats
	go func() {
		currentPoints := 0
		var m runtime.MemStats

		ticker := time.Tick(time.Duration(1) * time.Second)
		for {
			select {
			case <-ticker:
				points.Value(int64(currentPoints))
				runtime.ReadMemStats(&m)
				alloc.Value(int64(m.Alloc))
				totalAlloc.Value(int64(m.TotalAlloc))
				sysBytes.Value(int64(m.Sys))
				gcNum.Value(int64(m.NumGC))
				gcDur.Value(int64(m.PauseNs[(m.NumGC+255)%256]))
				var px int64
				if clusterStatus.IsPrimary() {
					px = 1
				} else {
					px = 0
				}
				clusterPrimary.Value(px)
			case update := <-totalPoints:
				currentPoints += update
			}
		}
	}()
}
