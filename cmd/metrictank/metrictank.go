package main

import (
	"context"
	"flag"
	"fmt"
	l "log"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Dieterbe/profiletrigger/heap"
	"github.com/Shopify/sarama"
	"github.com/grafana/globalconf"
	"github.com/grafana/metrictank/api"
	"github.com/grafana/metrictank/cluster"
	"github.com/grafana/metrictank/idx"
	"github.com/grafana/metrictank/idx/bigtable"
	"github.com/grafana/metrictank/idx/cassandra"
	"github.com/grafana/metrictank/idx/memory"
	metatagsBt "github.com/grafana/metrictank/idx/metatags/bigtable"
	metatagsCass "github.com/grafana/metrictank/idx/metatags/cassandra"
	"github.com/grafana/metrictank/input"
	inCarbon "github.com/grafana/metrictank/input/carbon"
	inKafkaMdm "github.com/grafana/metrictank/input/kafkamdm"
	"github.com/grafana/metrictank/jaeger"
	"github.com/grafana/metrictank/logger"
	"github.com/grafana/metrictank/mdata"
	"github.com/grafana/metrictank/mdata/cache"
	"github.com/grafana/metrictank/mdata/notifierKafka"
	"github.com/grafana/metrictank/stats"
	statsConfig "github.com/grafana/metrictank/stats/config"
	bigtableStore "github.com/grafana/metrictank/store/bigtable"
	cassandraStore "github.com/grafana/metrictank/store/cassandra"
	"github.com/grafana/metrictank/util"
	"github.com/raintank/dur"
	log "github.com/sirupsen/logrus"
)

var (
	warmupPeriod time.Duration
	startupTime  time.Time
	version      = "(none)"

	metrics     *mdata.AggMetrics
	metricIndex idx.MetricIndex
	apiServer   *api.Server
	inputs      []input.Plugin
	store       mdata.Store
	metaRecords idx.MetaRecordIdx

	// Misc:
	instance    = flag.String("instance", "default", "instance identifier. must be unique. used in clustering messages, for naming queue consumers and emitted metrics")
	showVersion = flag.Bool("version", false, "print version string")
	confFile    = flag.String("config", "/etc/metrictank/metrictank.ini", "configuration file path")

	// Data:
	dropFirstChunk    = flag.Bool("drop-first-chunk", false, "forego persisting of first received (and typically incomplete) chunk")
	ingestFromStr     = flag.String("ingest-from", "", "only ingest data for chunks that have a t0 equal or higher to the given timestamp. Specified per org. syntax: orgID:timestamp[,...]")
	chunkMaxStaleStr  = flag.String("chunk-max-stale", "1h", "max age for a chunk before to be considered stale and to be persisted to Cassandra.")
	metricMaxStaleStr = flag.String("metric-max-stale", "3h", "max age for a metric before to be considered stale and to be purged from memory.")
	gcIntervalStr     = flag.String("gc-interval", "1h", "Interval to run garbage collection job.")
	warmUpPeriodStr   = flag.String("warm-up-period", "1h", "duration until when secondary nodes are considered to have enough data to be ready and serve requests.")
	publicOrg         = flag.Int("public-org", 0, "org Id for publically (any org) accessible data. leave 0 to disable")

	// Profiling, instrumentation and logging:
	logLevel = flag.String("log-level", "info", "log level. panic|fatal|error|warning|info|debug")

	blockProfileRate = flag.Int("block-profile-rate", 0, "see https://golang.org/pkg/runtime/#SetBlockProfileRate")
	memProfileRate   = flag.Int("mem-profile-rate", 512*1024, "0 to disable. 1 for max precision (expensive!) see https://golang.org/pkg/runtime/#pkg-variables")

	proftrigPath       = flag.String("proftrigger-path", "/tmp", "path to store triggered profiles")
	proftrigFreqStr    = flag.String("proftrigger-freq", "60s", "inspect status frequency. set to 0 to disable")
	proftrigMinDiffStr = flag.String("proftrigger-min-diff", "1h", "minimum time between triggered profiles")
	proftrigHeapThresh = flag.Int("proftrigger-heap-thresh", 25000000000, "if this many bytes allocated, trigger a profile")
)

func main() {
	startupTime = time.Now()

	flag.Parse()

	// if the user just wants the version, give it and exit
	if *showVersion {
		fmt.Printf("metrictank (version: %s - runtime: %s)\n", version, runtime.Version())
		return
	}

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

	// input handlers
	input.ConfigSetup()

	// load config for metric ingestors
	inCarbon.ConfigSetup()
	inKafkaMdm.ConfigSetup()

	// load config for metricIndexers
	memory.ConfigSetup()
	cassandra.ConfigSetup()
	bigtable.ConfigSetup()

	// load config for API
	api.ConfigSetup()

	// load config for cluster
	cluster.ConfigSetup()

	// stats
	statsConfig.ConfigSetup()

	// storage-schemas, storage-aggregation files
	mdata.ConfigSetup()

	// cassandra Store
	cassandraStore.ConfigSetup()

	// bigtable store
	bigtableStore.ConfigSetup()

	// meta tag indexes
	metatagsCass.ConfigSetup()
	metatagsBt.ConfigSetup()

	jaeger.ConfigSetup()

	config.ParseAll()

	/***********************************
		Set up Logger
	***********************************/

	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	lvl, err := log.ParseLevel(*logLevel)
	if err != nil {
		log.Fatalf("failed to parse log-level, %s", err.Error())
	}
	log.SetLevel(lvl)
	log.Infof("logging level set to '%s'", *logLevel)

	/***********************************
		Validate settings needed for clustering
	***********************************/
	if *instance == "" {
		log.Fatal("instance can't be empty")
	}

	/***********************************
		Initialize our Cluster
	***********************************/
	api.ConfigProcess()
	cluster.ConfigProcess()
	scheme := "http"
	if api.UseSSL {
		scheme = "https"
	}
	addrParts := strings.Split(api.Addr, ":")
	port, err := strconv.ParseInt(addrParts[len(addrParts)-1], 10, 64)
	if err != nil {
		log.Fatalf("Could not parse port from listenAddr. %s", api.Addr)
	}
	cluster.Init(*instance, version, startupTime, scheme, int(port))

	/***********************************
		Validate remaining settings
	***********************************/
	inCarbon.ConfigProcess()
	inKafkaMdm.ConfigProcess(*instance)
	memory.ConfigProcess()
	notifierKafka.ConfigProcess(*instance)
	statsConfig.ConfigProcess(*instance)
	mdata.ConfigProcess()
	cassandra.ConfigProcess()
	bigtable.ConfigProcess()
	bigtableStore.ConfigProcess(mdata.MaxChunkSpan())
	jaeger.ConfigProcess()
	metatagsCass.ConfigProcess()
	metatagsBt.ConfigProcess()

	inputEnabled := inCarbon.Enabled || inKafkaMdm.Enabled
	wantInput := cluster.Mode == cluster.ModeDev || cluster.Mode == cluster.ModeShard
	if !inputEnabled && wantInput {
		log.Fatal("you should enable at least 1 input plugin in 'dev' or 'shard' cluster mode")
	}
	if inputEnabled && !wantInput {
		log.Fatal("you should not have an input enabled in 'query' cluster mode")
	}

	sec := dur.MustParseNDuration("warm-up-period", *warmUpPeriodStr)
	warmupPeriod = time.Duration(sec) * time.Second

	chunkMaxStale := dur.MustParseNDuration("chunk-max-stale", *chunkMaxStaleStr)
	metricMaxStale := dur.MustParseNDuration("metric-max-stale", *metricMaxStaleStr)
	gcInterval := time.Duration(dur.MustParseNDuration("gc-interval", *gcIntervalStr)) * time.Second

	proftrigFreq := dur.MustParseDuration("proftrigger-freq", *proftrigFreqStr)
	proftrigMinDiff := int(dur.MustParseNDuration("proftrigger-min-diff", *proftrigMinDiffStr))
	if proftrigFreq > 0 {
		errors := make(chan error)
		trigger, _ := heap.New(*proftrigPath, *proftrigHeapThresh, proftrigMinDiff, time.Duration(proftrigFreq)*time.Second, errors)
		go func() {
			for e := range errors {
				log.Errorf("profiletrigger heap: %s", e)
			}
		}()
		go trigger.Run()
	}

	/***********************************
		configure Profiling
	***********************************/
	runtime.SetBlockProfileRate(*blockProfileRate)
	runtime.MemProfileRate = *memProfileRate

	/************************************
	    handle interrupt signals
	************************************/
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	/***********************************
		Report Version
	***********************************/
	log.Infof("Metrictank starting. version: %s - runtime: %s", version, runtime.Version())
	// metric version.%s is the version of metrictank running.  The metric value is always 1
	mtVersion := stats.NewBool(fmt.Sprintf("version.%s", strings.Replace(version, ".", "_", -1)))
	mtVersion.Set(true)

	/***********************************
		collect stats
	***********************************/
	statsConfig.Start()

	/***********************************
		Initialize tracer
	***********************************/
	tracer, traceCloser, err := jaeger.Get()
	if err != nil {
		log.Fatalf("Could not initialize jaeger tracer: %s", err.Error())
	}
	defer traceCloser.Close()

	/***********************************
		Initialize our backendStore
	***********************************/
	if cassandraStore.CliConfig.Enabled && bigtableStore.CliConfig.Enabled {
		log.Fatal("only 1 backend store plugin can be enabled at once.")
	}
	if wantInput {
		if !cassandraStore.CliConfig.Enabled && !bigtableStore.CliConfig.Enabled {
			log.Fatal("at least 1 backend store plugin needs to be enabled in 'dev' or 'shard' cluster mode")
		}
	} else {
		if cassandraStore.CliConfig.Enabled || bigtableStore.CliConfig.Enabled {
			log.Fatal("no backend store plugin may be enabled in 'query' cluster mode")
		}
	}
	if bigtableStore.CliConfig.Enabled {
		schemaMaxChunkSpan := mdata.MaxChunkSpan()
		store, err = bigtableStore.NewStore(bigtableStore.CliConfig, mdata.TTLs(), schemaMaxChunkSpan)
		if err != nil {
			log.Fatalf("failed to initialize bigtable backend store. %s", err)
		}
		store.SetTracer(tracer)
	}
	if cassandraStore.CliConfig.Enabled {
		store, err = cassandraStore.NewCassandraStore(cassandraStore.CliConfig, mdata.TTLs())
		if err != nil {
			log.Fatalf("failed to initialize cassandra backend store. %s", err)
		}
		store.SetTracer(tracer)
	}

	/***********************************
		Initialize the Chunk Cache
	***********************************/
	var ccache *cache.CCache
	if inputEnabled {
		ccache = cache.NewCCache()
		ccache.SetTracer(tracer)
	}

	/***********************************
		Initialize our MemoryStore
	***********************************/

	ingestFrom, err := util.ParseIngestFromFlags(*ingestFromStr)
	if err != nil {
		log.Fatalf("ingest-from: %s", err.Error())
	}
	for orgID, timestamp := range ingestFrom {
		log.Infof("For org %d, will only ingest data for chunks that have a t0 equal or higher to %s", orgID, time.Unix(timestamp, 0))
	}
	if inputEnabled {
		metrics = mdata.NewAggMetrics(store, ccache, *dropFirstChunk, ingestFrom, chunkMaxStale, metricMaxStale, gcInterval)
	}

	/***********************************
		Initialize our Inputs
	***********************************/
	// note. all these New functions must either return a valid instance or call log.Fatal
	if inCarbon.Enabled {
		inputs = append(inputs, inCarbon.New())
	}

	if inKafkaMdm.Enabled {
		sarama.Logger = l.New(os.Stdout, "[Sarama] ", l.LstdFlags)
		inputs = append(inputs, inKafkaMdm.New())
	}

	if cluster.Mode == cluster.ModeShard && len(inputs) > 1 {
		log.Warn("It is not recommended to run a multi-node cluster with more than 1 input plugin.")
	}

	/***********************************
	    Start the ClusterManager
	***********************************/
	cluster.Start()

	/***********************************
		Initialize our MetricIdx
	***********************************/
	pre := time.Now()

	if *publicOrg < 0 {
		log.Fatal("public-org cannot be <0")
	}

	idx.OrgIdPublic = uint32(*publicOrg)

	idxEnabled := memory.Enabled || cassandra.CliConfig.Enabled || bigtable.CliConfig.Enabled
	if !idxEnabled && wantInput {
		log.Fatal("you should enable 1 index plugin in 'dev' or 'shard' cluster mode")
	}
	if idxEnabled && !wantInput {
		log.Fatal("you should not have an index plugin enabled in 'query' cluster mode")
	}

	var memIndex memory.MemoryIndex
	if memory.Enabled {
		memIndex = memory.New()
		metricIndex = memIndex
	}

	if cassandra.CliConfig.Enabled {
		if metricIndex != nil {
			log.Fatal("Only 1 metricIndex handler can be enabled.")
		}
		cassIdx := cassandra.New(cassandra.CliConfig)
		metricIndex = cassIdx
		memIndex = cassIdx.MemoryIndex
	}

	if bigtable.CliConfig.Enabled {
		if metricIndex != nil {
			log.Fatal("Only 1 metricIndex handler can be enabled.")
		}
		btIndex := bigtable.New(bigtable.CliConfig)
		metricIndex = btIndex
		memIndex = btIndex.MemoryIndex
	}

	if memory.TagSupport && memory.MetaTagSupport {
		if memory.Enabled {
			metaRecords = memIndex
		}

		if metatagsCass.CliConfig.Enabled {
			metatagsCassIdx := metatagsCass.NewCassandraMetaRecordIdx(metatagsCass.CliConfig, memIndex)
			err = metatagsCassIdx.Init()
			if err != nil {
				log.Fatalf("Failed to initialize cassandra meta tag index: %s", err)
			}
			metatagsCassIdx.Start()
			metaRecords = metatagsCassIdx
		}

		if metatagsBt.CliConfig.Enabled {
			metarecordBtIdx := metatagsBt.NewBigTableMetaRecordIdx(metatagsBt.CliConfig, memIndex)
			err = metarecordBtIdx.Init()
			if err != nil {
				log.Fatalf("Failed to initialize bigtable meta tag index: %s", err)
			}
			metarecordBtIdx.Start()
			metaRecords = metarecordBtIdx
		}
	}

	/***********************************
		Initialize our API server
	***********************************/
	apiServer, err = api.NewServer()
	if err != nil {
		log.Fatalf("Failed to start API. %s", err.Error())
	}

	apiServer.BindMetricIndex(metricIndex)
	apiServer.BindMemoryStore(metrics)
	apiServer.BindBackendStore(store)
	apiServer.BindMetaRecords(metaRecords)
	apiServer.BindCache(ccache)
	apiServer.BindTracer(tracer)
	cluster.Tracer = tracer
	go apiServer.Run()

	/***********************************
		Load index entries from the backend store.
	***********************************/
	if wantInput {
		err = metricIndex.Init()
		if err != nil {
			log.Fatalf("failed to initialize metricIndex: %s", err.Error())
		}
		log.Infof("metricIndex initialized in %s. starting data consumption", time.Now().Sub(pre))
	}

	/***********************************
		Initialize MetricPersist notifiers
	***********************************/
	var notifiers []mdata.Notifier
	if wantInput {
		if notifierKafka.Enabled {
			// The notifierKafka notifiers will block here until it has processed the backlog of metricPersist messages.
			// it will block for at most kafka-cluster.backlog-process-timeout (default 60s)
			notifiers = append(notifiers, notifierKafka.New(*instance, mdata.NewDefaultNotifierHandler(metrics, metricIndex)))
		}
		mdata.InitPersistNotifier(notifiers...)
	}
	if !wantInput && notifierKafka.Enabled {
		log.Fatal("you should disable notifier plugins in 'query' cluster mode")
	}

	/***********************************
		Start our inputs
	***********************************/
	ctx, cancel := context.WithCancel(context.Background())
	for _, plugin := range inputs {
		if carbonPlugin, ok := plugin.(*inCarbon.Carbon); ok {
			carbonPlugin.IntervalGetter(inCarbon.NewIndexIntervalGetter(metricIndex))
		}
		err = plugin.Start(input.NewDefaultHandler(metrics, metricIndex, plugin.Name()), cancel)
		if err != nil {
			shutdown()
			return
		}
		plugin.MaintainPriority()
		apiServer.BindPrioritySetter(plugin)
	}

	// metric cluster.self.promotion_wait is how long a candidate (secondary node) has to wait until it can become a primary
	// When the timer becomes 0 it means the in-memory buffer has been able to fully populate so that if you stop a primary
	// and it was able to save its complete chunks, this node will be able to take over without dataloss.
	// You can upgrade a candidate to primary while the timer is not 0 yet, it just means it may have missing data in the chunks that it will save.
	maxChunkSpan := mdata.MaxChunkSpan()
	stats.NewTimeDiffReporter32("cluster.self.promotion_wait", (uint32(time.Now().Unix())/maxChunkSpan+1)*maxChunkSpan)

	/***********************************
		Set our ready state so we can accept requests from users
		For this, both warm-up-period and gossip-settle-period must have lapsed
		(it is valid for either, or both to be 0)
	***********************************/
	waitWarmup := warmupPeriod
	waitSettle := cluster.GossipSettlePeriod

	// for primary nodes and query nodes, no warmup
	if cluster.Manager.IsPrimary() || !wantInput {
		waitWarmup = 0
	}
	wait := waitWarmup
	if waitSettle > waitWarmup {
		wait = waitSettle
	}

	log.Infof("Will set ready state after %s (warm-up-period %s, gossip-settle-period %s)", wait, warmupPeriod, cluster.GossipSettlePeriod)
	time.AfterFunc(wait, cluster.Manager.SetReady)

	/***********************************
		Wait for Shutdown
	***********************************/
	select {
	case sig := <-sigChan:
		log.Infof("Received signal %q. Shutting down", sig)
	case <-ctx.Done():
		log.Info("An input plugin signalled a fatal error. Shutting down")
	}
	shutdown()
}

func shutdown() {
	// Leave the cluster. All other nodes will be notified we have left
	// and so will stop sending us requests.
	cluster.Stop()

	// stop API
	apiServer.Stop()

	// shutdown our input plugins.  These may take a while as we allow them
	// to finish processing any metrics that have already been ingested.
	timer := time.NewTimer(time.Second * 10)
	var wg sync.WaitGroup
	for _, plugin := range inputs {
		wg.Add(1)
		go func(plugin input.Plugin) {
			log.Infof("Shutting down %s consumer", plugin.Name())
			plugin.Stop()
			log.Infof("%s consumer finished shutdown", plugin.Name())
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

	if cluster.Mode != cluster.ModeQuery {
		log.Info("closing store")
		store.Stop()
		log.Info("closing index")
		metricIndex.Stop()
	}
	log.Info("terminating.")
}
