package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/raintank/schema"

	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/logger"
	opentracing "github.com/opentracing/opentracing-go"
	log "github.com/sirupsen/logrus"

	"github.com/grafana/metrictank/store/cassandra"
	"github.com/raintank/dur"
	"github.com/rakyll/globalconf"
)

const tsFormat = "2006-01-02 15:04:05"

var (
	gitHash = "(none)"

	// flags from metrictank.go, globals
	showVersion = flag.Bool("version", false, "print version string")
	confFile    = flag.String("config", "/etc/metrictank/metrictank.ini", "configuration file path")

	// our own flags
	from        = flag.String("from", "-24h", "get data from (inclusive). only for points and point-summary format")
	to          = flag.String("to", "now", "get data until (exclusive). only for points and point-summary format")
	fix         = flag.Int("fix", 0, "fix data to this interval like metrictank does quantization. only for points and point-summary format")
	printTs     = flag.Bool("print-ts", false, "print time stamps instead of formatted dates. only for points and point-summary format")
	groupTTL    = flag.String("groupTTL", "d", "group chunks in TTL buckets: s (second. means unbucketed), m (minute), h (hour) or d (day). only for chunk-summary format")
	timeZoneStr = flag.String("time-zone", "local", "time-zone to use for interpreting from/to when needed. (check your config)")
	verbose     bool

	printTime func(ts uint32) string
)

func init() {
	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)
	flag.BoolVar(&verbose, "verbose", false, "verbose (print stuff about the request)")
}

func main() {
	storeConfig := cassandra.NewStoreConfig()
	// flags from cassandra/config.go, Cassandra
	flag.StringVar(&storeConfig.Addrs, "cassandra-addrs", storeConfig.Addrs, "cassandra host (may be given multiple times as comma-separated list)")
	flag.StringVar(&storeConfig.Keyspace, "cassandra-keyspace", storeConfig.Keyspace, "cassandra keyspace to use for storing the metric data table")
	flag.StringVar(&storeConfig.Consistency, "cassandra-consistency", storeConfig.Consistency, "write consistency (any|one|two|three|quorum|all|local_quorum|each_quorum|local_one")
	flag.StringVar(&storeConfig.HostSelectionPolicy, "cassandra-host-selection-policy", storeConfig.HostSelectionPolicy, "")
	flag.StringVar(&storeConfig.Timeout, "cassandra-timeout", storeConfig.Timeout, "cassandra timeout")
	flag.IntVar(&storeConfig.ReadConcurrency, "cassandra-read-concurrency", storeConfig.ReadConcurrency, "max number of concurrent reads to cassandra.")
	//flag.IntVar(&storeConfig.WriteConcurrency, "write-concurrency", storeConfig.WriteConcurrency, "max number of concurrent writes to cassandra.")
	flag.IntVar(&storeConfig.ReadQueueSize, "cassandra-read-queue-size", storeConfig.ReadQueueSize, "max number of outstanding reads before reads will be dropped. This is important if you run queries that result in many reads in parallel.")
	//flag.IntVar(&storeConfig.WriteQueueSize, "write-queue-size", storeConfig.WriteQueueSize, "write queue size per cassandra worker. should be large engough to hold all at least the total number of series expected, divided by how many workers you have")
	flag.IntVar(&storeConfig.Retries, "cassandra-retries", storeConfig.Retries, "how many times to retry a query before failing it")
	flag.IntVar(&storeConfig.WindowFactor, "window-factor", storeConfig.WindowFactor, "size of compaction window relative to TTL")
	flag.StringVar(&storeConfig.OmitReadTimeout, "cassandra-omit-read-timeout", storeConfig.OmitReadTimeout, "if a read is older than this, it will directly be omitted without executing")
	flag.IntVar(&storeConfig.CqlProtocolVersion, "cql-protocol-version", storeConfig.CqlProtocolVersion, "cql protocol version to use")
	flag.BoolVar(&storeConfig.CreateKeyspace, "cassandra-create-keyspace", storeConfig.CreateKeyspace, "enable the creation of the mdata keyspace and tables, only one node needs this")
	flag.BoolVar(&storeConfig.DisableInitialHostLookup, "cassandra-disable-initial-host-lookup", storeConfig.DisableInitialHostLookup, "instruct the driver to not attempt to get host info from the system.peers table")
	flag.BoolVar(&storeConfig.SSL, "cassandra-ssl", storeConfig.SSL, "enable SSL connection to cassandra")
	flag.StringVar(&storeConfig.CaPath, "cassandra-ca-path", storeConfig.CaPath, "cassandra CA certificate path when using SSL")
	flag.BoolVar(&storeConfig.HostVerification, "cassandra-host-verification", storeConfig.HostVerification, "host (hostname and server cert) verification when using SSL")
	flag.BoolVar(&storeConfig.Auth, "cassandra-auth", storeConfig.Auth, "enable cassandra authentication")
	flag.StringVar(&storeConfig.Username, "cassandra-username", storeConfig.Username, "username for authentication")
	flag.StringVar(&storeConfig.Password, "cassandra-password", storeConfig.Password, "password for authentication")
	flag.StringVar(&storeConfig.SchemaFile, "cassandra-schema-file", storeConfig.SchemaFile, "File containing the needed schemas in case database needs initializing")

	flag.Usage = func() {
		fmt.Println("mt-store-cat")
		fmt.Println()
		fmt.Println("Retrieves timeseries data from the cassandra store. Either raw or with minimal processing")
		fmt.Println()
		fmt.Println("Usage:")
		fmt.Println()
		fmt.Printf("	mt-store-cat [flags] tables\n")
		fmt.Println()
		fmt.Printf("	mt-store-cat [flags] <table-selector> <metric-selector> <format>\n")
		fmt.Printf("	                     table-selector: '*' or name of a table. e.g. 'metric_128'\n")
		fmt.Printf("	                     metric-selector: '*' or an id (of raw or aggregated series) or prefix:<prefix> or substr:<substring> or glob:<pattern>\n")
		fmt.Printf("	                     format:\n")
		fmt.Printf("	                            - points\n")
		fmt.Printf("	                            - point-summary\n")
		fmt.Printf("	                            - chunk-summary (shows TTL's, optionally bucketed. See groupTTL flag)\n")
		fmt.Printf("	                            - chunk-csv (for importing into cassandra)\n")
		fmt.Println()
		fmt.Println("EXAMPLES:")
		fmt.Println("mt-store-cat -cassandra-keyspace metrictank -from='-1min' '*' '1.77c8c77afa22b67ef5b700c2a2b88d5f' points")
		fmt.Println("mt-store-cat -cassandra-keyspace metrictank -from='-1month' '*' 'prefix:fake' point-summary")
		fmt.Println("mt-store-cat -cassandra-keyspace metrictank '*' 'prefix:fake' chunk-summary")
		fmt.Println("mt-store-cat -groupTTL h -cassandra-keyspace metrictank 'metric_512' '1.37cf8e3731ee4c79063c1d55280d1bbe' chunk-summary")
		fmt.Println("Flags:")
		flag.PrintDefaults()
		fmt.Println("Notes:")
		fmt.Println(" * Using `*` as metric-selector may bring down your cassandra. Especially chunk-summary ignores from/to and queries all data.")
		fmt.Println("   With great power comes great responsibility")
		fmt.Println(" * points that are not in the `from <= ts < to` range, are prefixed with `-`. In range has prefix of '>`")
		fmt.Println(" * When using chunk-summary, if there's data that should have been expired by cassandra, but for some reason didn't, we won't see or report it")
		fmt.Println(" * Doesn't automatically return data for aggregated series. It's up to you to query for an AMKey (id_<rollup>_<span>) when appropriate")
		fmt.Println(" * (rollup is one of sum, cnt, lst, max, min and span is a number in seconds)")
	}
	flag.Parse()

	if *showVersion {
		fmt.Printf("mt-store-cat (built with %s, git hash %s)\n", runtime.Version(), gitHash)
		return
	}
	if flag.NArg() < 1 {
		flag.Usage()
		os.Exit(-1)
	}
	var tableSelector, metricSelector, format string
	tableSelector = flag.Arg(0)
	if tableSelector != "tables" {
		if flag.NArg() < 3 {
			flag.Usage()
			os.Exit(-1)
		}
		metricSelector = flag.Arg(1)
		format = flag.Arg(2)
		if format != "points" && format != "point-summary" && format != "chunk-summary" && format != "chunk-csv" {
			flag.Usage()
			os.Exit(-1)
		}
		if metricSelector == "prefix:" || metricSelector == "substr:" || metricSelector == "glob:" {
			log.Fatal("prefix/substr/glob cannot be empty")
		}
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
		log.Fatalf("error with configuration file: %s", err.Error())
		os.Exit(1)
	}

	config.ParseAll()

	if *groupTTL != "s" && *groupTTL != "m" && *groupTTL != "h" && *groupTTL != "d" {
		log.Fatal("groupTTL must be one of s/m/h/d")
		os.Exit(1)
	}

	if *showVersion {
		fmt.Printf("mt-store-cat (built with %s, git hash %s)\n", runtime.Version(), gitHash)
		return
	}

	printTime = printTimeFormatted
	if *printTs {
		printTime = printTimeUnix
	}

	var loc *time.Location
	switch *timeZoneStr {
	case "local":
		loc = time.Local
	default:
		var err error
		loc, err = time.LoadLocation(*timeZoneStr)
		if err != nil {
			log.Fatal(err.Error())
		}
	}

	store, err := cassandra.NewCassandraStore(storeConfig, nil)
	if err != nil {
		log.Fatalf("failed to initialize cassandra. %s", err.Error())
	}
	tracer, traceCloser, err := conf.GetTracer(false, "", nil)
	if err != nil {
		log.Fatalf("Could not initialize jaeger tracer: %s", err.Error())
	}
	defer traceCloser.Close()
	store.SetTracer(tracer)

	err = store.FindExistingTables(storeConfig.Keyspace)
	if err != nil {
		log.Fatalf("failed to read tables from cassandra. %s", err.Error())
	}

	// set up is done, now actually execute the business logic

	// handle "tables"
	if tableSelector == "tables" {
		printTables(store)
		return
	}

	// handle the case where we have a table-selector, metric-selector and format
	// table-selector: '*' or name of a table. e.g. 'metric_128'
	// metric-selector: '*' or an id (of raw or aggregated series) or prefix:<prefix> or substr:<substring> or glob:<pattern>
	// format: points, point-summary, chunk-summary or chunk-csv

	if format == "chunk-csv" && (tableSelector == "*" || tableSelector == "") {
		log.Fatal("chunk-csv format can be used with 1 cassandra table only")
	}

	tables, err := getTables(store, tableSelector)
	if err != nil {
		log.Fatal(err.Error())
	}

	var fromUnix, toUnix uint32

	if format == "points" || format == "point-summary" || format == "chunk-csv" {
		now := time.Now()
		defaultFrom := uint32(now.Add(-time.Duration(24) * time.Hour).Unix())
		defaultTo := uint32(now.Add(time.Duration(1) * time.Second).Unix())

		fromUnix, err = dur.ParseDateTime(*from, loc, now, defaultFrom)
		if err != nil {
			log.Fatal(err.Error())
		}

		toUnix, err = dur.ParseDateTime(*to, loc, now, defaultTo)
		if err != nil {
			log.Fatal(err.Error())
		}
	}
	var metrics []Metric
	if metricSelector == "*" {
		if verbose {
			fmt.Println("# Looking for ALL metrics")
		}
		// chunk-summary doesn't need an explicit listing. it knows if metrics is empty, to query all
		// but the other two do need an explicit listing.
		if format == "points" || format == "point-summary" {
			metrics, err = getMetrics(store, "", "", "")
			if err != nil {
				log.Errorf("cassandra query error. %s", err.Error())
				return
			}
		}
	} else if strings.HasPrefix(metricSelector, "prefix:") || strings.HasPrefix(metricSelector, "substr:") || strings.HasPrefix(metricSelector, "glob:") {
		var prefix, substr, glob string
		if strings.HasPrefix(metricSelector, "prefix:") {
			prefix = strings.Replace(metricSelector, "prefix:", "", 1)
		}
		if strings.HasPrefix(metricSelector, "substr:") {
			substr = strings.Replace(metricSelector, "substr:", "", 1)
		}
		if strings.HasPrefix(metricSelector, "glob:") {
			glob = strings.Replace(metricSelector, "glob:", "", 1)
		}
		if verbose {
			fmt.Println("# Looking for these metrics:")
		}
		metrics, err = getMetrics(store, prefix, substr, glob)
		if err != nil {
			log.Errorf("cassandra query error. %s", err.Error())
			return
		}
		if verbose {
			for _, m := range metrics {
				fmt.Println(m)
			}
		}
	} else {
		amkey, err := schema.AMKeyFromString(metricSelector)
		if err != nil {
			log.Errorf("can't parse metric selector as AMKey: %s", err.Error())
			return
		}

		if verbose {
			fmt.Println("# Looking for this metric:")
		}

		metrics, err = getMetric(store, amkey)
		if err != nil {
			log.Errorf("cassandra query error. %s", err.Error())
			return
		}
		if len(metrics) == 0 {
			fmt.Printf("metric id %v not found", amkey.MKey)
			return
		}
		if verbose {
			for _, m := range metrics {
				fmt.Println(m)
			}
		}
	}

	if verbose {
		fmt.Printf("# Keyspace %q:\n", storeConfig.Keyspace)
	}

	span := tracer.StartSpan("mt-store-cat " + format)
	ctx := opentracing.ContextWithSpan(context.Background(), span)

	switch format {
	case "points":
		printPoints(ctx, store, tables, metrics, fromUnix, toUnix, uint32(*fix))
	case "point-summary":
		printPointSummary(ctx, store, tables, metrics, fromUnix, toUnix, uint32(*fix))
	case "chunk-summary":
		printChunkSummary(ctx, store, tables, metrics, *groupTTL)
	case "chunk-csv":
		printChunkCsv(ctx, store, tables[0], metrics, fromUnix, toUnix)
	}
}
