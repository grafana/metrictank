package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"runtime"
	"strings"
	"syscall"
	"time"

	"net"
	"net/http"
	_ "net/http/pprof"

	met "github.com/grafana/grafana/pkg/metric"
	"github.com/grafana/grafana/pkg/metric/helper"
	"github.com/nsqio/go-nsq"
	"github.com/raintank/raintank-metric/app"
	"github.com/raintank/raintank-metric/instrumented_nsq"
	gometrics "github.com/rcrowley/go-metrics"
)

var (
	showVersion = flag.Bool("version", false, "print version string")
	dryRun      = flag.Bool("dry", false, "dry run (disable actually storing into cassandra")

	// TODO split up for consumer and cassandra sender
	concurrency = flag.Int("concurrency", 10, "number of workers parsing messages and writing into cassandra. also number of nsq consumers for both high and low prio topic")
	topic       = flag.String("topic", "metrics", "NSQ topic")
	channel     = flag.String("channel", "tank", "NSQ channel")
	instance    = flag.String("instance", "default", "instance, to separate instances in metrics")
	maxInFlight = flag.Int("max-in-flight", 200, "max number of messages to allow in flight")
	chunkSpan   = flag.Int("chunkspan", 120, "chunk span in seconds")
	numChunks   = flag.Int("numchunks", 5, "number of chunks to keep in memory. should be at least 1 more than what's needed to satisfy aggregation rules")
	metricTTL   = flag.Int("ttl", 3024000, "seconds before metrics are removed from cassandra")

	cassandraPort = flag.Int("cassandra-port", 9042, "cassandra port")
	listenAddr    = flag.String("listen", ":6060", "http listener address.")

	statsdAddr = flag.String("statsd-addr", "localhost:8125", "statsd address")
	statsdType = flag.String("statsd-type", "standard", "statsd type: standard or datadog")

	cassandraAddrs   = app.StringArray{}
	consumerOpts     = app.StringArray{}
	producerOpts     = app.StringArray{}
	nsqdTCPAddrs     = app.StringArray{}
	lookupdHTTPAddrs = app.StringArray{}

	metrics *AggMetrics
)

func init() {
	flag.Var(&consumerOpts, "consumer-opt", "option to passthrough to nsq.Consumer (may be given multiple times, http://godoc.org/github.com/nsqio/go-nsq#Config)")
	flag.Var(&producerOpts, "producer-opt", "option to passthrough to nsq.Producer (may be given multiple times, see http://godoc.org/github.com/nsqio/go-nsq#Config)")

	flag.Var(&nsqdTCPAddrs, "nsqd-tcp-address", "nsqd TCP address (may be given multiple times)")
	flag.Var(&lookupdHTTPAddrs, "lookupd-http-address", "lookupd HTTP address (may be given multiple times)")
	flag.Var(&cassandraAddrs, "cassandra-addrs", "cassandra host (may be given multiple times)")
}

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

func main() {
	flag.Parse()

	if *showVersion {
		fmt.Println("nsq_metrics_tank")
		return
	}
	if *instance == "" {
		log.Fatal("instance can't be empty")
	}
	hostname, err := os.Hostname()
	if err != nil {
		log.Fatal(err)
	}
	stats, err := helper.New(true, *statsdAddr, *statsdType, "nsq_metrics_tank", strings.Replace(hostname, ".", "_", -1))
	if err != nil {
		log.Fatal(err)
	}

	if *channel == "" {
		rand.Seed(time.Now().UnixNano())
		*channel = fmt.Sprintf("tail%06d#ephemeral", rand.Int()%999999)
	}

	if *topic == "" {
		log.Fatal("--topic is required")
	}

	if len(nsqdTCPAddrs) == 0 && len(lookupdHTTPAddrs) == 0 {
		log.Fatal("--nsqd-tcp-address or --lookupd-http-address required")
	}
	if len(nsqdTCPAddrs) > 0 && len(lookupdHTTPAddrs) > 0 {
		log.Fatal("use --nsqd-tcp-address or --lookupd-http-address not both")
	}
	// set default cassandra address if none is set.
	if len(cassandraAddrs) == 0 {
		cassandraAddrs = append(cassandraAddrs, "localhost")
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	cfg := nsq.NewConfig()
	cfg.UserAgent = "nsq_metrics_tank"
	err = app.ParseOpts(cfg, consumerOpts)
	if err != nil {
		log.Fatal(err)
	}
	cfg.MaxInFlight = *maxInFlight

	consumer, err := insq.NewConsumer(*topic, *channel, cfg, "%s", stats)
	if err != nil {
		log.Fatal(err)
	}

	pCfg := nsq.NewConfig()
	pCfg.UserAgent = "nsq_metrics_tank"
	err = app.ParseOpts(pCfg, producerOpts)
	if err != nil {
		log.Fatal(err)
	}

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

	err = InitCassandra()

	if err != nil {
		log.Fatal(err)
	}

	metrics = NewAggMetrics(uint32(*chunkSpan), uint32(*numChunks), uint32(300), uint32(3600*2), 1)
	handler := NewHandler(metrics)
	consumer.AddConcurrentHandlers(handler, *concurrency)

	err = consumer.ConnectToNSQDs(nsqdTCPAddrs)
	if err != nil {
		log.Fatal(err)
	}
	log.Println("INFO : connected to nsqd")

	err = consumer.ConnectToNSQLookupds(lookupdHTTPAddrs)
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		alloc := gometrics.NewGauge()
		totalAlloc := gometrics.NewGauge()
		sys := gometrics.NewGauge()
		gometrics.Register("bytes_alloc_not_freed", alloc)
		gometrics.Register("bytes_alloc_incl_freed", totalAlloc)
		gometrics.Register("bytes_sys", sys)
		m := &runtime.MemStats{}
		for range time.Tick(time.Duration(1) * time.Second) {
			runtime.ReadMemStats(m)
			alloc.Update(int64(m.Alloc))
			totalAlloc.Update(int64(m.TotalAlloc))
			sys.Update(int64(m.Sys))
		}
	}()

	addr, _ := net.ResolveTCPAddr("tcp", "influxdb:2003")
	go gometrics.Graphite(gometrics.DefaultRegistry, 10e9, fmt.Sprintf("metrics.nsq_metrics_tank.%s.", *instance), addr)

	go func() {
		http.HandleFunc("/get", Get)
		log.Println("INFO starting listener for metrics and http/debug on ", *listenAddr)
		log.Println(http.ListenAndServe(*listenAddr, nil))
	}()

	for {
		select {
		case <-consumer.StopChan:
			err := metrics.Persist()
			if err != nil {
				log.Printf("Error: failed to persist aggmetrics. %v", err)
			}
			cSession.Close()
			return
		case <-sigChan:
			consumer.Stop()
		}
	}
}
