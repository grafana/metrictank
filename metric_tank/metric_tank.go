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

	"github.com/Dieterbe/profiletrigger/heap"
	"github.com/benbjohnson/clock"
	"github.com/raintank/met"
	"github.com/raintank/met/helper"
	"github.com/raintank/raintank-metric/dur"
	"github.com/raintank/raintank-metric/metric_tank/defcache"
	inKafkaMdam "github.com/raintank/raintank-metric/metric_tank/in/kafkamdam"
	inKafkaMdm "github.com/raintank/raintank-metric/metric_tank/in/kafkamdm"
	inNSQ "github.com/raintank/raintank-metric/metric_tank/in/nsq"
	"github.com/raintank/raintank-metric/metric_tank/mdata"
	"github.com/raintank/raintank-metric/metric_tank/mdata/chunk"
	"github.com/raintank/raintank-metric/metric_tank/usage"
	"github.com/raintank/raintank-metric/metricdef"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"
)

var (
	logLevel     int
	warmupPeriod time.Duration
	startupTime  time.Time
	GitHash      = "(none)"

	metrics  *mdata.AggMetrics
	defCache *defcache.DefCache

	showVersion = flag.Bool("version", false, "print version string")
	listenAddr  = flag.String("listen", ":6060", "http listener address.")
	confFile    = flag.String("config", "/etc/raintank/metric_tank.ini", "configuration file path")

	concurrency        = flag.Int("concurrency", 10, "number of workers parsing messages from NSQ")
	topic              = flag.String("topic", "metrics", "NSQ topic for metrics")
	topicNotifyPersist = flag.String("topic-notify-persist", "metricpersist", "NSQ topic for persist messages")
	channel            = flag.String("channel", "tank", "NSQ channel for both metric topic and metric-persist topic")
	maxInFlight        = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")

	instance         = flag.String("instance", "default", "cluster node name and value used to differentiate metrics between nodes")
	blockProfileRate = flag.Int("block-profile-rate", 0, "see https://golang.org/pkg/runtime/#SetBlockProfileRate")
	memProfileRate   = flag.Int("mem-profile-rate", 512*1024, "0 to disable. 1 for max precision (expensive!) see https://golang.org/pkg/runtime/#pkg-variables")
	primaryNode      = flag.Bool("primary-node", false, "the primary node writes data to cassandra. There should only be 1 primary node per cluster of nodes.")

	chunkSpanStr = flag.String("chunkspan", "2h", "chunk span")
	numChunksInt = flag.Int("numchunks", 5, "number of chunks to keep in memory. should be at least 1 more than what's needed to satisfy aggregation rules")
	ttlStr       = flag.String("ttl", "35d", "minimum wait before metrics are removed from cassandra")

	chunkMaxStaleStr  = flag.String("chunk-max-stale", "1h", "max age for a chunk before to be considered stale and to be persisted to Cassandra.")
	metricMaxStaleStr = flag.String("metric-max-stale", "6h", "max age for a metric before to be considered stale and to be purged from memory.")
	gcIntervalStr     = flag.String("gc-interval", "1h", "Interval to run garbage collection job.")
	warmUpPeriodStr   = flag.String("warm-up-period", "1h", "duration before secondary nodes start serving requests")

	aggSettings = flag.String("agg-settings", "", "aggregation settings: <agg span in seconds>:<agg chunkspan in seconds>:<agg numchunks>:<ttl in seconds>[:<ready as bool. default true>] (may be given multiple times as comma-separated list)")

	cassandraAddrs            = flag.String("cassandra-addrs", "", "cassandra host (may be given multiple times as comma-separated list)")
	cassandraConsistency      = flag.String("cassandra-consistency", "one", "write consistency (any|one|two|three|quorum|all|local_quorum|each_quorum|local_one")
	cassandraTimeout          = flag.Int("cassandra-timeout", 1000, "cassandra timeout in milliseconds")
	cassandraReadConcurrency  = flag.Int("cassandra-read-concurrency", 20, "max number of concurrent reads to cassandra.")
	cassandraWriteConcurrency = flag.Int("cassandra-write-concurrency", 10, "max number of concurrent writes to cassandra.")
	cassandraReadQueueSize    = flag.Int("cassandra-read-queue-size", 100, "max number of outstanding reads before blocking. value doesn't matter much")
	cassandraWriteQueueSize   = flag.Int("cassandra-write-queue-size", 100000, "write queue size per cassandra worker. should be large engough to hold all at least the total number of series expected, divided by how many workers you have")

	esAddr    = flag.String("elastic-addr", "localhost:9200", "elasticsearch address for metric definitions")
	indexName = flag.String("index-name", "metric", "Elasticsearch index name for storing metric index.")

	statsdAddr = flag.String("statsd-addr", "localhost:8125", "statsd address")
	statsdType = flag.String("statsd-type", "standard", "statsd type: standard or datadog")

	accountingPeriodStr = flag.String("accounting-period", "5min", "accounting period to track per-org usage metrics")

	proftrigPath       = flag.String("proftrigger-path", "/tmp", "path to store triggered profiles")
	proftrigFreqStr    = flag.String("proftrigger-freq", "60s", "inspect status frequency. set to 0 to disable")
	proftrigMinDiffStr = flag.String("proftrigger-min-diff", "1h", "minimum time between triggered profiles")
	proftrigHeapThresh = flag.Int("proftrigger-heap-thresh", 10000000, "if this many bytes allocated, trigger a profile")

	logMinDurStr = flag.String("log-min-dur", "5min", "only log incoming requests if their timerange is at least this duration. Use 0 to disable")

	producerOpts     = flag.String("producer-opt", "", "option to passthrough to nsq.Producer (may be given multiple times as comma-separated list, see http://godoc.org/github.com/nsqio/go-nsq#Config)")
	consumerOpts     = flag.String("consumer-opt", "", "option to passthrough to nsq.Consumer (may be given multiple times as comma-separated list, http://godoc.org/github.com/nsqio/go-nsq#Config)")
	nsqdTCPAddrs     = flag.String("nsqd-tcp-address", "", "nsqd TCP address (may be given multiple times as comma-separated list)")
	lookupdHTTPAddrs = flag.String("lookupd-http-address", "", "lookupd HTTP address (may be given multiple times as comma-separated list)")

	kafkaMdmBroker  = flag.String("kafka-mdm-broker", "", "tcp address for kafka, for MetricData messages, msgp encoded")
	kafkaMdamBroker = flag.String("kafka-mdam-broker", "", "tcp address for kafka, for MetricDataArray messages, msgp encoded")

	reqSpanMem  met.Meter
	reqSpanBoth met.Meter

	cassWriteQueueSize    met.Gauge
	cassWriters           met.Gauge
	getTargetDuration     met.Timer
	itersToPointsDuration met.Timer
	messagesSize          met.Meter
	// just 1 global timer of request handling time. includes mem/cassandra gets, chunk decode/iters, json building etc
	// there is such a thing as too many metrics.  we have this, and cassandra timings, that should be enough for realtime profiling
	reqHandleDuration met.Timer
	inItems           met.Meter
	points            met.Gauge
	msgsHandleOK      met.Count
	msgsHandleFail    met.Count
	alloc             met.Gauge
	totalAlloc        met.Gauge
	sysBytes          met.Gauge
	clusterPrimary    met.Gauge
	clusterPromoWait  met.Gauge
	gcNum             met.Gauge // go GC
	gcDur             met.Gauge // go GC
	gcCpuFraction     met.Gauge // go GC

	promotionReadyAtChan chan uint32
)

func init() {
	flag.IntVar(&logLevel, "log-level", 2, "log level. 0=TRACE|1=DEBUG|2=INFO|3=WARN|4=ERROR|5=CRITICAL|6=FATAL")
}

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

	log.NewLogger(0, "console", fmt.Sprintf(`{"level": %d, "formatting":false}`, logLevel))
	mdata.LogLevel = logLevel
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

	if *showVersion {
		fmt.Printf("metrics_tank (built with %s, git hash %s)\n", runtime.Version(), GitHash)
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
	runtime.SetBlockProfileRate(*blockProfileRate)
	runtime.MemProfileRate = *memProfileRate
	mdata.InitMetrics(stats)

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

	defs, err := metricdef.NewDefsEs(*esAddr, "", "", *indexName, nil)
	if err != nil {
		log.Fatal(4, "failed to initialize Elasticsearch. %s", err)
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	sec := dur.MustParseUNsec("warm-up-period", *warmUpPeriodStr)
	warmupPeriod = time.Duration(sec) * time.Second

	// set our cluster state before we start consuming messages.
	mdata.CluStatus = mdata.NewClusterStatus(*instance, *primaryNode)

	promotionReadyAtChan = make(chan uint32)
	initMetrics(stats)

	logMinDur := dur.MustParseUsec("log-min-dur", *logMinDurStr)
	chunkSpan := dur.MustParseUNsec("chunkspan", *chunkSpanStr)
	numChunks := uint32(*numChunksInt)
	chunkMaxStale := dur.MustParseUNsec("chunk-max-stale", *chunkMaxStaleStr)
	metricMaxStale := dur.MustParseUNsec("metric-max-stale", *metricMaxStaleStr)
	gcInterval := time.Duration(dur.MustParseUNsec("gc-interval", *gcIntervalStr)) * time.Second
	ttl := dur.MustParseUNsec("ttl", *ttlStr)
	if (mdata.Month_sec % chunkSpan) != 0 {
		panic("chunkSpan must fit without remainders into month_sec (28*24*60*60)")
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
		highestChunkSpan = max(highestChunkSpan, aggChunkSpan)
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

	store, err := mdata.NewCassandraStore(stats, *cassandraAddrs, *cassandraConsistency, *cassandraTimeout, *cassandraReadConcurrency, *cassandraWriteConcurrency, *cassandraReadQueueSize, *cassandraWriteQueueSize)
	if err != nil {
		log.Fatal(4, "failed to initialize cassandra. %s", err)
	}
	store.InitMetrics(stats)

	nsqdAdds := strings.Split(*nsqdTCPAddrs, ",")
	if len(nsqdAdds) == 1 && nsqdAdds[0] == "" {
		nsqdAdds = []string{}
	}

	lookupdAdds := strings.Split(*lookupdHTTPAddrs, ",")
	if len(lookupdAdds) == 1 && lookupdAdds[0] == "" {
		lookupdAdds = []string{}
	}

	var nsq *inNSQ.NSQ
	var kafkaMdm *inKafkaMdm.KafkaMdm
	var kafkaMdam *inKafkaMdam.KafkaMdam

	if *nsqdTCPAddrs != "" || *lookupdHTTPAddrs != "" {
		nsq = inNSQ.New(*consumerOpts, *nsqdTCPAddrs, *lookupdHTTPAddrs, *topic, *channel, *maxInFlight, *concurrency, stats)
	}

	if *kafkaMdmBroker != "" {
		kafkaMdm = inKafkaMdm.New(*kafkaMdmBroker, "mdm", *instance, stats)
	}
	if *kafkaMdamBroker != "" {
		kafkaMdam = inKafkaMdam.New(*kafkaMdamBroker, "mdam", *instance, stats)
	}

	accountingPeriod := dur.MustParseUNsec("accounting-period", *accountingPeriodStr)

	log.Info("Metric tank starting. Built from %s - Go version %s", GitHash, runtime.Version())

	metrics = mdata.NewAggMetrics(store, chunkSpan, numChunks, chunkMaxStale, metricMaxStale, ttl, gcInterval, finalSettings)
	pre := time.Now()
	defCache = defcache.New(defs, stats)
	usg := usage.New(accountingPeriod, metrics, defCache, clock.New())

	log.Info("DefCache initialized in %s. starting data consumption", time.Now().Sub(pre))

	if *nsqdTCPAddrs != "" || *lookupdHTTPAddrs != "" {
		nsq.Start(metrics, defCache, usg)
	}
	if kafkaMdm != nil {
		kafkaMdm.Start(metrics, defCache, usg)
	}
	if kafkaMdam != nil {
		kafkaMdam.Start(metrics, defCache, usg)
	}
	promotionReadyAtChan <- (uint32(time.Now().Unix())/highestChunkSpan + 1) * highestChunkSpan

	mdata.InitCluster(metrics, stats, *instance, *topicNotifyPersist, *channel, *producerOpts, *consumerOpts, nsqdAdds, lookupdAdds, *maxInFlight)

	go func() {
		http.HandleFunc("/", appStatus)
		http.HandleFunc("/get", get(store, defCache, finalSettings, logMinDur))                        // metric-tank native api which deals with ID's, not target strings
		http.HandleFunc("/get/", get(store, defCache, finalSettings, logMinDur))                       // metric-tank native api which deals with ID's, not target strings
		http.HandleFunc("/render", corsHandler(getLegacy(store, defCache, finalSettings, logMinDur)))  // traditional graphite api
		http.HandleFunc("/render/", corsHandler(getLegacy(store, defCache, finalSettings, logMinDur))) // traditional graphite api
		http.HandleFunc("/metrics/index.json", corsHandler(IndexJson(defCache)))
		http.HandleFunc("/metrics/find", corsHandler(findHandler))
		http.HandleFunc("/metrics/find/", corsHandler(findHandler))
		http.HandleFunc("/cluster", mdata.CluStatus.HttpHandler)
		http.HandleFunc("/cluster/", mdata.CluStatus.HttpHandler)
		log.Info("starting listener for metrics and http/debug on %s", *listenAddr)
		log.Info("%s", http.ListenAndServe(*listenAddr, nil))
	}()

	type ingestPlugin interface {
		Stop()
	}

	type waiter struct {
		key    string
		plugin ingestPlugin
		ch     chan int
	}

	waiters := make([]waiter, 0)

	if nsq != nil {
		waiters = append(waiters, waiter{
			"nsq",
			nsq,
			nsq.StopChan,
		})
	}
	if kafkaMdm != nil {
		waiters = append(waiters, waiter{
			"kafka-mdm",
			kafkaMdm,
			kafkaMdm.StopChan,
		})
	}
	if kafkaMdam != nil {
		waiters = append(waiters, waiter{
			"kafka-mdam",
			kafkaMdam,
			kafkaMdam.StopChan,
		})
	}
	<-sigChan
	for _, w := range waiters {
		log.Info("Shutting down %s consumer", w.key)
		w.plugin.Stop()
	}
	for _, w := range waiters {
		// the order here is arbitrary, they could stop in either order, but it doesn't really matter
		log.Info("waiting for %s consumer to finish shutdown", w.key)
		<-w.ch
		log.Info("%s consumer finished shutdown", w.key)
	}
	log.Info("closing store")
	store.Stop()
	defs.Stop()
	log.Info("terminating.")
	log.Close()

}

func initMetrics(stats met.Backend) {
	reqSpanMem = stats.NewMeter("requests_span.mem", 0)
	reqSpanBoth = stats.NewMeter("requests_span.mem_and_cassandra", 0)
	cassWriteQueueSize = stats.NewGauge("cassandra.write_queue.size", int64(*cassandraWriteQueueSize))
	cassWriters = stats.NewGauge("cassandra.num_writers", int64(*cassandraWriteConcurrency))
	getTargetDuration = stats.NewTimer("get_target_duration", 0)
	itersToPointsDuration = stats.NewTimer("iters_to_points_duration", 0)
	messagesSize = stats.NewMeter("message_size", 0)
	reqHandleDuration = stats.NewTimer("request_handle_duration", 0)
	inItems = stats.NewMeter("in.items", 0)
	points = stats.NewGauge("total_points", 0)
	msgsHandleOK = stats.NewCount("handle.ok")
	msgsHandleFail = stats.NewCount("handle.fail")
	alloc = stats.NewGauge("bytes_alloc.not_freed", 0)
	totalAlloc = stats.NewGauge("bytes_alloc.incl_freed", 0)
	sysBytes = stats.NewGauge("bytes_sys", 0)
	clusterPrimary = stats.NewGauge("cluster.primary", 0)
	clusterPromoWait = stats.NewGauge("cluster.promotion_wait", 1)
	gcNum = stats.NewGauge("gc.num", 0)
	gcDur = stats.NewGauge("gc.dur", 0)                 // in nanoseconds. last known duration.
	gcCpuFraction = stats.NewGauge("gc.cpufraction", 0) // reported as pro-mille

	// run a collector for some global stats
	go func() {
		currentPoints := 0
		var m runtime.MemStats
		var promotionReadyAtTs uint32

		ticker := time.Tick(time.Duration(1) * time.Second)
		for {
			select {
			case now := <-ticker:
				points.Value(int64(currentPoints))
				runtime.ReadMemStats(&m)
				alloc.Value(int64(m.Alloc))
				totalAlloc.Value(int64(m.TotalAlloc))
				sysBytes.Value(int64(m.Sys))
				gcNum.Value(int64(m.NumGC))
				gcDur.Value(int64(m.PauseNs[(m.NumGC+255)%256]))
				gcCpuFraction.Value(int64(1000 * m.GCCPUFraction))
				var px int64
				if mdata.CluStatus.IsPrimary() {
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
			case update := <-chunk.TotalPoints:
				currentPoints += update
			case promotionReadyAtTs = <-promotionReadyAtChan:
			}
		}
	}()
}
