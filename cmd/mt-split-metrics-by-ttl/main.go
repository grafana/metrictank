package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/raintank/dur"
	"github.com/raintank/metrictank/mdata"
)

var (
	// flags from metrictank.go, Cassandra
	cassandraAddrs               = flag.String("cassandra-addrs", "localhost", "cassandra host (may be given multiple times as comma-separated list)")
	cassandraKeyspace            = flag.String("cassandra-keyspace", "metrictank", "cassandra keyspace to use for storing the metric data table")
	cassandraConsistency         = flag.String("cassandra-consistency", "one", "write consistency (any|one|two|three|quorum|all|local_quorum|each_quorum|local_one")
	cassandraHostSelectionPolicy = flag.String("cassandra-host-selection-policy", "tokenaware,hostpool-epsilon-greedy", "")
	cassandraTimeout             = flag.Int("cassandra-timeout", 1000, "cassandra timeout in milliseconds")
	cassandraReadConcurrency     = flag.Int("cassandra-read-concurrency", 20, "max number of concurrent reads to cassandra.")
	cassandraReadQueueSize       = flag.Int("cassandra-read-queue-size", 100, "max number of outstanding reads before blocking. value doesn't matter much")
	cassandraRetries             = flag.Int("cassandra-retries", 0, "how many times to retry a query before failing it")
	cqlProtocolVersion           = flag.Int("cql-protocol-version", 4, "cql protocol version to use")

	cassandraSSL              = flag.Bool("cassandra-ssl", false, "enable SSL connection to cassandra")
	cassandraCaPath           = flag.String("cassandra-ca-path", "/etc/metrictank/ca.pem", "cassandra CA certificate path when using SSL")
	cassandraHostVerification = flag.Bool("cassandra-host-verification", true, "host (hostname and server cert) verification when using SSL")

	cassandraAuth     = flag.Bool("cassandra-auth", false, "enable cassandra authentication")
	cassandraUsername = flag.String("cassandra-username", "cassandra", "username for authentication")
	cassandraPassword = flag.String("cassandra-password", "cassandra", "password for authentication")

	windowFactor             = flag.Int("window-factor", 20, "the window factor be used when creating the metric table schema")
	cassandraOmitReadTimeout = flag.Int("cassandra-omit-read-timeout", 10, "if a read is older than this, it will directly be omitted without executing")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "mt-split-metrics-by-ttl [flags] ttl [ttl...]")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Creates schema of metric tables split by TTLs and")
		fmt.Fprintln(os.Stderr, "assists in migrating the data to new tables.")
		fmt.Println("Flags:")
		flag.PrintDefaults()
		os.Exit(-1)
	}
	flag.Parse()

	var ttls []uint32
	if flag.NArg() < 1 {
		flag.Usage()
	}

	for i := 0; i < flag.NArg(); i++ {
		ttls = append(ttls, uint32(dur.MustParseNDuration("ttl", flag.Arg(i))))
	}

	tmpDir, err := ioutil.TempDir(os.TempDir(), *cassandraKeyspace)
	if err != nil {
		panic(fmt.Sprintf("Failed to get temp dir: %s", tmpDir))
	}
	snapshotDir := path.Join(tmpDir, "snapshot")
	err = os.Mkdir(snapshotDir, 0700)
	if err != nil {
		panic(fmt.Sprintf("Error creating directory: %s", err))
	}

	store, err := mdata.NewCassandraStore(*cassandraAddrs, *cassandraKeyspace, *cassandraConsistency, *cassandraCaPath, *cassandraUsername, *cassandraPassword, *cassandraHostSelectionPolicy, *cassandraTimeout, *cassandraReadConcurrency, *cassandraReadConcurrency, *cassandraReadQueueSize, 0, *cassandraRetries, *cqlProtocolVersion, *windowFactor, *cassandraOmitReadTimeout, *cassandraSSL, *cassandraAuth, *cassandraHostVerification, ttls)
	if err != nil {
		panic(fmt.Sprintf("Failed to instantiate cassandra: %s", err))
	}
	tables := store.GetTableNames()

	// create directory/link structure that we need to define the future table names
	err = os.Mkdir(path.Join(tmpDir, *cassandraKeyspace), 0700)
	if err != nil {
		panic(fmt.Sprintf("Failed to create directory: %s", err))
	}
	namedTableLinks := make([]string, len(tables))
	for i, table := range tables {
		namedTableLinks[i] = path.Join(tmpDir, *cassandraKeyspace, table)
		err := os.Symlink(snapshotDir, namedTableLinks[i])
		if err != nil {
			panic(fmt.Sprintf("Error when creating symlink: %s", err))
		}
	}

	fmt.Printf("The following tables have been created: %s\n", strings.Join(tables, ", "))
	fmt.Println("Now continue with the following steps on your cassandra node(s):")
	fmt.Println("- Recreate (or copy) this directory structure on each cassandra node:")
	fmt.Printf("  %s\n", tmpDir)
	fmt.Println("- Stop writes by stopping the metrictanks or set their cluster status to secondary")
	fmt.Printf("- Use `nodetool snapshot --table metric %s` on all cluster nodes to create\n", *cassandraKeyspace)
	fmt.Println("  a snapshot of the metric table")
	fmt.Println("  https://docs.datastax.com/en/cassandra/3.0/cassandra/tools/toolsSnapShot.html")
	fmt.Println(fmt.Sprintf("- Move snapshot files to directory %s", snapshotDir))
	fmt.Println("- Load the data by executing:")
	for _, dir := range namedTableLinks {
		fmt.Println(fmt.Sprintf("  sstableloader -d %s %s", *cassandraAddrs, dir))
	}
}
