package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/raintank/dur"
	"github.com/raintank/metrictank/mdata"
	"github.com/rakyll/globalconf"
)

const tsFormat = "2006-01-02 15:04:05"

var (
	GitHash = "(none)"

	// flags from metrictank.go, globals
	showVersion = flag.Bool("version", false, "print version string")
	confFile    = flag.String("config", "/etc/metrictank/metrictank.ini", "configuration file path")

	// flags from metrictank.go, Cassandra
	cassandraAddrs               = flag.String("cassandra-addrs", "localhost", "cassandra host (may be given multiple times as comma-separated list)")
	cassandraKeyspace            = flag.String("cassandra-keyspace", "raintank", "cassandra keyspace to use for storing the metric data table")
	cassandraConsistency         = flag.String("cassandra-consistency", "one", "write consistency (any|one|two|three|quorum|all|local_quorum|each_quorum|local_one")
	cassandraHostSelectionPolicy = flag.String("cassandra-host-selection-policy", "tokenaware,hostpool-epsilon-greedy", "")
	cassandraTimeout             = flag.Int("cassandra-timeout", 1000, "cassandra timeout in milliseconds")
	cassandraReadConcurrency     = flag.Int("cassandra-read-concurrency", 20, "max number of concurrent reads to cassandra.")
	//cassandraWriteConcurrency    = flag.Int("cassandra-write-concurrency", 10, "max number of concurrent writes to cassandra.")
	cassandraReadQueueSize = flag.Int("cassandra-read-queue-size", 100, "max number of outstanding reads before blocking. value doesn't matter much")
	//cassandraWriteQueueSize      = flag.Int("cassandra-write-queue-size", 100000, "write queue size per cassandra worker. should be large engough to hold all at least the total number of series expected, divided by how many workers you have")
	cassandraRetries   = flag.Int("cassandra-retries", 0, "how many times to retry a query before failing it")
	cqlProtocolVersion = flag.Int("cql-protocol-version", 4, "cql protocol version to use")

	cassandraSSL              = flag.Bool("cassandra-ssl", false, "enable SSL connection to cassandra")
	cassandraCaPath           = flag.String("cassandra-ca-path", "/etc/metrictank/ca.pem", "cassandra CA certificate path when using SSL")
	cassandraHostVerification = flag.Bool("cassandra-host-verification", true, "host (hostname and server cert) verification when using SSL")

	cassandraAuth     = flag.Bool("cassandra-auth", false, "enable cassandra authentication")
	cassandraUsername = flag.String("cassandra-username", "cassandra", "username for authentication")
	cassandraPassword = flag.String("cassandra-password", "cassandra", "password for authentication")

	// our own flags
	from                     = flag.String("from", "-24h", "get data from (inclusive). only for points and points-summary format")
	to                       = flag.String("to", "now", "get data until (exclusive). only for points and points-summary format")
	fix                      = flag.Int("fix", 0, "fix data to this interval like metrictank does quantization. only for points and points-summary format")
	printTs                  = flag.Bool("print-ts", false, "print time stamps instead of formatted dates. only for points and poins-summary format")
	groupTTL                 = flag.String("groupTTL", "d", "group chunks in TTL buckets based on s (second. means unbucketed), m (minute), h (hour) or d (day). only for chunk-summary format")
	windowFactor             = flag.Int("window-factor", 20, "the window factor be used when creating the metric table schema")
	timeZoneStr              = flag.String("time-zone", "local", "time-zone to use for interpreting from/to when needed. (check your config)")
	cassandraOmitReadTimeout = flag.Int("cassandra-omit-read-timeout", 10, "if a read is older than this, it will directly be omitted without executing")
)

func main() {
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
		fmt.Printf("	                     metric-selector: '*' or an id (of raw or aggregated series) or prefix:<prefix>\n")
		fmt.Printf("	                     format:\n")
		fmt.Printf("	                            - points\n")
		fmt.Printf("	                            - point-summary\n")
		fmt.Printf("	                            - chunk-summary (shows TTL's, optionally bucketed. See groupTTL flag)\n")
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
		fmt.Println(" * Doesn't automatically return data for aggregated series. It's up to you to query for id_<rollup>_<span> when appropriate")
		fmt.Println(" * (rollup is one of sum, cnt, lst, max, min and span is a number in seconds)")
	}
	flag.Parse()

	if *showVersion {
		fmt.Printf("mt-store-cat (built with %s, git hash %s)\n", runtime.Version(), GitHash)
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
		if format != "points" && format != "point-summary" && format != "chunk-summary" {
			flag.Usage()
			os.Exit(-1)
		}
		if metricSelector == "prefix:" {
			log.Fatal("prefix cannot be empty")
		}

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

	conf.ParseAll()

	if *groupTTL != "s" && *groupTTL != "m" && *groupTTL != "h" && *groupTTL != "d" {
		log.Fatal(4, "groupTTL must be one of s/m/h/d")
		os.Exit(1)
	}

	if *showVersion {
		fmt.Printf("mt-store-cat (built with %s, git hash %s)\n", runtime.Version(), GitHash)
		return
	}

	var loc *time.Location
	switch *timeZoneStr {
	case "local":
		loc = time.Local
	default:
		var err error
		loc, err = time.LoadLocation(*timeZoneStr)
		if err != nil {
			log.Fatal(err)
		}
	}

	store, err := mdata.NewCassandraStore(*cassandraAddrs, *cassandraKeyspace, *cassandraConsistency, *cassandraCaPath, *cassandraUsername, *cassandraPassword, *cassandraHostSelectionPolicy, *cassandraTimeout, *cassandraReadConcurrency, *cassandraReadConcurrency, *cassandraReadQueueSize, 0, *cassandraRetries, *cqlProtocolVersion, *windowFactor, *cassandraOmitReadTimeout, *cassandraSSL, *cassandraAuth, *cassandraHostVerification, nil)
	if err != nil {
		log.Fatal(4, "failed to initialize cassandra. %s", err)
	}

	if tableSelector == "tables" {
		tables, err := getTables(store, *cassandraKeyspace, "")
		if err != nil {
			log.Fatal(4, "%s", err)
		}
		for _, tbl := range tables {
			fmt.Println(tbl)
		}
		return
	}
	tables, err := getTables(store, *cassandraKeyspace, tableSelector)
	if err != nil {
		log.Fatal(4, "%s", err)
	}

	var fromUnix, toUnix uint32

	if format == "points" || format == "point-summary" {
		now := time.Now()
		defaultFrom := uint32(now.Add(-time.Duration(24) * time.Hour).Unix())
		defaultTo := uint32(now.Add(time.Duration(1) * time.Second).Unix())

		fromUnix, err = dur.ParseDateTime(*from, loc, now, defaultFrom)
		if err != nil {
			log.Fatal(err)
		}

		toUnix, err = dur.ParseDateTime(*to, loc, now, defaultTo)
		if err != nil {
			log.Fatal(err)
		}
	}
	var metrics []Metric
	if metricSelector == "*" {
		fmt.Println("# Looking for ALL metrics")
		// chunk-summary doesn't need an explicit listing. it knows if metrics is empty, to query all
		// but the other two do need an explicit listing.
		if format == "points" || format == "point-summary" {
			metrics, err = getMetrics(store, "")
			if err != nil {
				log.Error(3, "cassandra query error. %s", err)
				return
			}
		}
	} else if strings.HasPrefix(metricSelector, "prefix:") {
		fmt.Println("# Looking for these metrics:")
		metrics, err = getMetrics(store, strings.Replace(metricSelector, "prefix:", "", 1))
		if err != nil {
			log.Error(3, "cassandra query error. %s", err)
			return
		}
		for _, m := range metrics {
			fmt.Println(m.id, m.name)
		}
	} else {
		fmt.Println("# Looking for this metric:")

		id := metricSelector

		// the input selector is an aggregated series like '12574.d144038944994c54b892ae33e3d8802b_sum_600'
		// for the query lookup, strip off the last bits since aggregated series have no index entry.
		if strings.Count(metricSelector, "_") == 2 {
			id = metricSelector[:strings.Index(metricSelector, "_")]
		}
		metrics, err = getMetric(store, id)
		if err != nil {
			log.Error(3, "cassandra query error. %s", err)
			return
		}
		if len(metrics) == 0 {
			fmt.Printf("metric id %q not found", id)
			return
		}
		for i, m := range metrics {
			if m.id != metricSelector {
				fmt.Println(metricSelector, "(base", m.id, ")", m.name)
				metrics[i].id = metricSelector // this is what we'll actually have to search for
			} else {
				fmt.Println(m.id, m.name)
			}
		}
	}

	fmt.Printf("# Keyspace %q:\n", *cassandraKeyspace)

	switch format {
	case "points":
		points(store, tables, metrics, fromUnix, toUnix, uint32(*fix))
	case "point-summary":
		pointSummary(store, tables, metrics, fromUnix, toUnix, uint32(*fix))
	case "chunk-summary":
		chunkSummary(store, tables, metrics, *cassandraKeyspace, *groupTTL)
	}
}
