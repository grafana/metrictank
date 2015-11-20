package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"net/http"
	_ "net/http/pprof"

	"github.com/grafana/grafana/pkg/log"
	met "github.com/grafana/grafana/pkg/metric"
	"github.com/grafana/grafana/pkg/metric/helper"
	"github.com/nsqio/go-nsq"
	"github.com/raintank/raintank-metric/app"
	"github.com/raintank/raintank-metric/instrumented_nsq"
	"github.com/rakyll/globalconf"
)

var (
	showVersion = flag.Bool("version", false, "print version string")
	dryRun      = flag.Bool("dry", false, "dry run (disable actually storing into cassandra")

	concurrency = flag.Int("concurrency", 10, "number of workers parsing messages")
	topic       = flag.String("topic", "metrics", "NSQ topic")
	channel     = flag.String("channel", "tank", "NSQ channel")
	instance    = flag.String("instance", "default", "instance, to separate instances in metrics")
	maxInFlight = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")
	chunkSpan   = flag.Int("chunkspan", 120, "chunk span in seconds")
	numChunks   = flag.Int("numchunks", 5, "number of chunks to keep in memory. should be at least 1 more than what's needed to satisfy aggregation rules")

	cassandraWriteConcurrency = flag.Int("cassandra-write-concurrency", 50, "max number of concurrent writes to cassandra.")
	cassandraPort             = flag.Int("cassandra-port", 9042, "cassandra port")
	cassandraAddrs            = flag.String("cassandra-addrs", "", "cassandra host (may be given multiple times as comma-separated list)")
	metricTTL                 = flag.Int("ttl", 3024000, "seconds before metrics are removed from cassandra")

	listenAddr = flag.String("listen", ":6060", "http listener address.")

	statsdAddr = flag.String("statsd-addr", "localhost:8125", "statsd address")
	statsdType = flag.String("statsd-type", "standard", "statsd type: standard or datadog")

	dumpFile = flag.String("dump-file", "/tmp/nmt.gob", "path of file to dump of all metrics written at shutdown and read at startup")

	gcInterval     = flag.Int("gc-interval", 3600, "Interval in seconds to run garbage collection job.")
	chunkMaxStale  = flag.Int("chunk-max-stale", 3600, "maximum number of seconds before a stale chunk is persisted to Cassandra.")
	metricMaxStale = flag.Int("metric-max-stale", 21600, "maximum number of seconds before a stale metric is purged from memory.")

	logLevel = flag.Int("log-level", 2, "log level. 0=TRACE|1=DEBUG|2=INFO|3=WARN|4=ERROR|5=CRITICAL|6=FATAL")

	confFile = flag.String("config", "/etc/raintank/metric_tank.ini", "configuration file path")

	consumerOpts     = flag.String("consumer-opt", "", "option to passthrough to nsq.Consumer (may be given multiple times as comma-separated list, http://godoc.org/github.com/nsqio/go-nsq#Config)")
	nsqdTCPAddrs     = flag.String("nsqd-tcp-address", "", "nsqd TCP address (may be given multiple times as comma-separated list)")
	lookupdHTTPAddrs = flag.String("lookupd-http-address", "", "lookupd HTTP address (may be given multiple times as comma-separated list)")

	metrics *AggMetrics
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
var cassandraGetDuration met.Timer
var inItems met.Meter
var msgsHandleOK met.Count
var msgsHandleFail met.Count
var alloc met.Gauge
var totalAlloc met.Gauge
var sysBytes met.Gauge

func main() {
	flag.Parse()

	// Only try and parse the conf file if it exists
	if _, err := os.Stat(*confFile); err == nil {
		conf, err := globalconf.NewWithOptions(&globalconf.Options{Filename: *confFile})
		if err != nil {
			log.Fatal(0, "error with configuration file: %s", err)
			os.Exit(1)
		}
		conf.ParseAll()
	}

	log.NewLogger(0, "console", fmt.Sprintf(`{"level": %d, "formatting":true}`, *logLevel))

	if *showVersion {
		fmt.Println("nsq_metrics_tank")
		return
	}
	if *instance == "" {
		log.Fatal(0, "instance can't be empty")
	}
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(0, "failed to lookup hostname. %s", err)
	}
	stats, err := helper.New(true, *statsdAddr, *statsdType, "metric_tank", strings.Replace(hostname, ".", "_", -1))
	if err != nil {
		log.Fatal(0, "failed to initialize statsd. %s", err)
	}

	if *channel == "" {
		rand.Seed(time.Now().UnixNano())
		*channel = fmt.Sprintf("tail%06d#ephemeral", rand.Int()%999999)
	}

	if *topic == "" {
		log.Fatal(0, "--topic is required")
	}

	if *nsqdTCPAddrs == "" && *lookupdHTTPAddrs == "" {
		log.Fatal(0, "--nsqd-tcp-address or --lookupd-http-address required")
	}
	if *nsqdTCPAddrs != "" && *lookupdHTTPAddrs != "" {
		log.Fatal(0, "use --nsqd-tcp-address or --lookupd-http-address not both")
	}
	// set default cassandra address if none is set.
	if *cassandraAddrs == "" {
		*cassandraAddrs = "localhost"
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	cfg := nsq.NewConfig()
	cfg.UserAgent = "nsq_metrics_tank"
	err = app.ParseOpts(cfg, *consumerOpts)
	if err != nil {
		log.Fatal(0, "failed to parse nsq consumer options. %s", err)
	}
	cfg.MaxInFlight = *maxInFlight

	consumer, err := insq.NewConsumer(*topic, *channel, cfg, "%s", stats)
	if err != nil {
		log.Fatal(0, "Failed to create NSQ consumer. %s", err)
	}

	initMetrics(stats)

	err = InitCassandra()

	if err != nil {
		log.Fatal(0, "failed to initialize cassandra. %s", err)
	}

	metrics = NewAggMetrics(uint32(*chunkSpan), uint32(*numChunks), uint32(300), uint32(3600*2), 1)
	handler := NewHandler(metrics)
	consumer.AddConcurrentHandlers(handler, *concurrency)

	nsqdAdds := strings.Split(*nsqdTCPAddrs, ",")
	if len(nsqdAdds) == 1 && nsqdAdds[0] == "" {
		nsqdAdds = []string{}
	}
	err = consumer.ConnectToNSQDs(nsqdAdds)
	if err != nil {
		log.Fatal(0, "failed to connect to NSQDs. %s", err)
	}
	log.Info("connected to nsqd")

	lookupdAdds := strings.Split(*lookupdHTTPAddrs, ",")
	if len(lookupdAdds) == 1 && lookupdAdds[0] == "" {
		lookupdAdds = []string{}
	}
	err = consumer.ConnectToNSQLookupds(lookupdAdds)
	if err != nil {
		log.Fatal(0, "failed to connect to NSQLookupds. %s", err)
	}

	go func() {
		m := &runtime.MemStats{}
		for range time.Tick(time.Duration(1) * time.Second) {
			runtime.ReadMemStats(m)
			alloc.Value(int64(m.Alloc))
			totalAlloc.Value(int64(m.TotalAlloc))
			sysBytes.Value(int64(m.Sys))
		}
	}()

	go func() {
		http.HandleFunc("/get", Get)
		log.Info("starting listener for metrics and http/debug on %s", *listenAddr)
		log.Info("%s", http.ListenAndServe(*listenAddr, nil))
	}()

	for {
		select {
		case <-consumer.StopChan:
			err := metrics.Persist()
			if err != nil {
				log.Error(0, "failed to persist aggmetrics. %s", err)
			}
			log.Info("closing cassandra session.")
			cSession.Close()
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
	cassandraPutDuration = stats.NewTimer("cassandra_put_duration", 0)
	inItems = stats.NewMeter("in.items", 0)
	msgsHandleOK = stats.NewCount("handle.ok")
	msgsHandleFail = stats.NewCount("handle.fail")
	alloc = stats.NewGauge("bytes_alloc.not_freed", 0)
	totalAlloc = stats.NewGauge("bytes_alloc.incl_freed", 0)
	sysBytes = stats.NewGauge("bytes_sys", 0)
}
