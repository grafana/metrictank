package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/grafana/metrictank/store/cassandra"
	"github.com/raintank/dur"
)

func main() {
	storeConfig := cassandra.NewStoreConfig()
	// we don't need to allocate resources for reads as this tool does not read from the Store
	storeConfig.ReadConcurrency = 1
	storeConfig.ReadQueueSize = 0

	// flags from cassandra/config.go
	flag.StringVar(&storeConfig.Addrs, "cassandra-addrs", storeConfig.Addrs, "cassandra host (may be given multiple times as comma-separated list)")
	flag.StringVar(&storeConfig.Keyspace, "cassandra-keyspace", storeConfig.Keyspace, "cassandra keyspace to use for storing the metric data table")
	flag.StringVar(&storeConfig.Consistency, "cassandra-consistency", storeConfig.Consistency, "write consistency (any|one|two|three|quorum|all|local_quorum|each_quorum|local_one")
	flag.StringVar(&storeConfig.HostSelectionPolicy, "cassandra-host-selection-policy", storeConfig.HostSelectionPolicy, "")
	flag.IntVar(&storeConfig.Timeout, "cassandra-timeout", storeConfig.Timeout, "cassandra timeout in milliseconds")
	flag.IntVar(&storeConfig.Retries, "cassandra-retries", storeConfig.Retries, "how many times to retry a query before failing it")
	flag.IntVar(&storeConfig.CqlProtocolVersion, "cql-protocol-version", storeConfig.CqlProtocolVersion, "cql protocol version to use")
	flag.BoolVar(&storeConfig.DisableInitialHostLookup, "cassandra-disable-initial-host-lookup", storeConfig.DisableInitialHostLookup, "instruct the driver to not attempt to get host info from the system.peers table")
	flag.BoolVar(&storeConfig.SSL, "cassandra-ssl", storeConfig.SSL, "enable SSL connection to cassandra")
	flag.StringVar(&storeConfig.CaPath, "cassandra-ca-path", storeConfig.CaPath, "cassandra CA certificate path when using SSL")
	flag.BoolVar(&storeConfig.HostVerification, "cassandra-host-verification", storeConfig.HostVerification, "host (hostname and server cert) verification when using SSL")
	flag.BoolVar(&storeConfig.Auth, "cassandra-auth", storeConfig.Auth, "enable cassandra authentication")
	flag.StringVar(&storeConfig.Username, "cassandra-username", storeConfig.Username, "username for authentication")
	flag.StringVar(&storeConfig.Password, "cassandra-password", storeConfig.Password, "password for authentication")

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

	tmpDir, err := ioutil.TempDir(os.TempDir(), storeConfig.Keyspace)
	if err != nil {
		panic(fmt.Sprintf("Failed to get temp dir: %s", tmpDir))
	}
	snapshotDir := path.Join(tmpDir, "snapshot")
	err = os.Mkdir(snapshotDir, 0700)
	if err != nil {
		panic(fmt.Sprintf("Error creating directory: %s", err))
	}

	store, err := cassandra.NewCassandraStore(storeConfig, ttls)
	if err != nil {
		panic(fmt.Sprintf("Failed to instantiate cassandra: %s", err))
	}
	tables := store.GetTableNames()

	// create directory/link structure that we need to define the future table names
	err = os.Mkdir(path.Join(tmpDir, storeConfig.Keyspace), 0700)
	if err != nil {
		panic(fmt.Sprintf("Failed to create directory: %s", err))
	}
	namedTableLinks := make([]string, len(tables))
	for i, table := range tables {
		namedTableLinks[i] = path.Join(tmpDir, storeConfig.Keyspace, table)
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
	fmt.Printf("- Use `nodetool snapshot --table metric %s` on all cluster nodes to create\n", storeConfig.Keyspace)
	fmt.Println("  a snapshot of the metric table")
	fmt.Println("  https://docs.datastax.com/en/cassandra/3.0/cassandra/tools/toolsSnapShot.html")
	fmt.Println(fmt.Sprintf("- Move snapshot files to directory %s", snapshotDir))
	fmt.Println("- Load the data by executing:")
	for _, dir := range namedTableLinks {
		fmt.Println(fmt.Sprintf("  sstableloader -d %s %s", storeConfig.Addrs, dir))
	}
}
