package main

import (
	"flag"
	"fmt"
	"os"
	"runtime"
	"strconv"
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
	cassandraHostSelectionPolicy = flag.String("cassandra-host-selection-policy", "roundrobin", "")
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
	from         = flag.String("from", "-24h", "get data from (inclusive)")
	to           = flag.String("to", "now", "get data until (exclusive)")
	table        = flag.String("table", "*", "which table to query.  e.g. 'metric_128'. '*' means all of them")
	mdp          = flag.Int("mdp", 0, "max data points to return")
	fix          = flag.Int("fix", 0, "fix data to this interval like metrictank does quantization")
	windowFactor = flag.Int("window-factor", 20, "the window factor be used when creating the metric table schema")
	printTs      = flag.Bool("print-ts", false, "print time stamps instead of formatted dates")
)

func main() {
	flag.Usage = func() {
		fmt.Println("mt-store-cat")
		fmt.Println()
		fmt.Println("Retrieves timeseries data from the cassandra store. Either raw or with minimal processing")
		fmt.Println()
		fmt.Println("Usage:")
		fmt.Printf("	mt-store-cat [flags] <normal|summary> id <metric-id>\n")
		fmt.Printf("	mt-store-cat [flags] <normal|summary> query <org-id> <graphite query> (not supported yet)\n")
		fmt.Printf("	mt-store-cat [flags] <normal|summary> full [roundTTL. defaults to 3600] [prefix match]\n")
		fmt.Println("Flags:")
		flag.PrintDefaults()
		fmt.Println("Notes:")
		fmt.Println(" * points that are not in the `from <= ts < to` range, are prefixed with `-`. In range has prefix of '>`")
	}
	flag.Parse()

	if *showVersion {
		fmt.Printf("mt-store-cat (built with %s, git hash %s)\n", runtime.Version(), GitHash)
		return
	}
	if flag.NArg() < 2 {
		flag.Usage()
		os.Exit(-1)
	}

	selector := flag.Arg(1)
	var id string
	var roundTTL = 3600
	var prefix string
	// var query string
	// var org int

	switch selector {
	case "id":
		if flag.NArg() < 3 {
			flag.Usage()
			os.Exit(-1)
		}

		id = flag.Arg(2)
	case "full":
		if flag.NArg() >= 3 {
			var err error
			roundTTL, err = strconv.Atoi(flag.Arg(2))
			if err != nil {
				flag.Usage()
				os.Exit(-1)
			}
		}
		if flag.NArg() == 4 {
			prefix = flag.Arg(3)
		}
		if flag.NArg() > 4 {
			flag.Usage()
			os.Exit(-1)
		}

	case "query":
		//		if flag.NArg() < 4 {
		//			flag.Usage()
		//			os.Exit(-1)
		//		}
		//		org64, err := strconv.ParseInt(flag.Arg(3), 10, 32)
		//		if err != nil {
		//			flag.Usage()
		//			os.Exit(-1)
		//		}
		//		org = int(org64)
		//		query = flag.Arg(4)
		panic("sorry, queries not supported yet")
	default:
		flag.Usage()
		os.Exit(-1)
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

	if *showVersion {
		fmt.Printf("mt-store-cat (built with %s, git hash %s)\n", runtime.Version(), GitHash)
		return
	}

	mode := flag.Arg(0)
	if mode != "normal" && mode != "summary" {
		panic("unsupported mode " + mode)
	}

	store, err := mdata.NewCassandraStore(*cassandraAddrs, *cassandraKeyspace, *cassandraConsistency, *cassandraCaPath, *cassandraUsername, *cassandraPassword, *cassandraHostSelectionPolicy, *cassandraTimeout, *cassandraReadConcurrency, *cassandraReadConcurrency, *cassandraReadQueueSize, 0, *cassandraRetries, *cqlProtocolVersion, *windowFactor, *cassandraSSL, *cassandraAuth, *cassandraHostVerification, nil)
	if err != nil {
		log.Fatal(4, "failed to initialize cassandra. %s", err)
	}

	tables, err := getTables(store, *cassandraKeyspace, *table)
	if err != nil {
		log.Fatal(4, "%s", err)
	}

	switch selector {
	case "id":
		now := time.Now()
		defaultFrom := uint32(now.Add(-time.Duration(24) * time.Hour).Unix())
		defaultTo := uint32(now.Add(time.Duration(1) * time.Second).Unix())

		fromUnix, err := dur.ParseTSpec(*from, now, defaultFrom)
		if err != nil {
			log.Fatal(err)
		}

		toUnix, err := dur.ParseTSpec(*to, now, defaultTo)
		if err != nil {
			log.Fatal(err)
		}
		catId(store, tables, id, fromUnix, toUnix, uint32(*fix), mode)
	case "full":
		Dump(store, tables, *cassandraKeyspace, prefix, roundTTL)
	}
}
