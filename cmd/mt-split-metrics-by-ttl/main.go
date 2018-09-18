package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"path"
	"strings"

	"github.com/grafana/metrictank/logger"
	"github.com/grafana/metrictank/store/cassandra"
	"github.com/raintank/dur"
	log "github.com/sirupsen/logrus"
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
	flag.StringVar(&storeConfig.Timeout, "cassandra-timeout", storeConfig.Timeout, "cassandra timeout")
	flag.IntVar(&storeConfig.Retries, "cassandra-retries", storeConfig.Retries, "how many times to retry a query before failing it")
	flag.IntVar(&storeConfig.CqlProtocolVersion, "cql-protocol-version", storeConfig.CqlProtocolVersion, "cql protocol version to use")
	flag.BoolVar(&storeConfig.DisableInitialHostLookup, "cassandra-disable-initial-host-lookup", storeConfig.DisableInitialHostLookup, "instruct the driver to not attempt to get host info from the system.peers table")
	flag.BoolVar(&storeConfig.SSL, "cassandra-ssl", storeConfig.SSL, "enable SSL connection to cassandra")
	flag.StringVar(&storeConfig.CaPath, "cassandra-ca-path", storeConfig.CaPath, "cassandra CA certificate path when using SSL")
	flag.BoolVar(&storeConfig.HostVerification, "cassandra-host-verification", storeConfig.HostVerification, "host (hostname and server cert) verification when using SSL")
	flag.BoolVar(&storeConfig.Auth, "cassandra-auth", storeConfig.Auth, "enable cassandra authentication")
	flag.StringVar(&storeConfig.Username, "cassandra-username", storeConfig.Username, "username for authentication")
	flag.StringVar(&storeConfig.Password, "cassandra-password", storeConfig.Password, "password for authentication")

	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	formatter.QuoteEmptyFields = true

	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)

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
		log.WithFields(log.Fields{
			"directory": tmpDir,
		}).Panic("failed to get temporary directory")
	}
	snapshotDir := path.Join(tmpDir, "snapshot")
	err = os.Mkdir(snapshotDir, 0700)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Panic("failed to create snapshot directory")
	}

	store, err := cassandra.NewCassandraStore(storeConfig, ttls)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Panic("failed to instantiate cassandra")
	}

	// create directory/link structure that we need to define the future table names
	err = os.Mkdir(path.Join(tmpDir, storeConfig.Keyspace), 0700)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Panic("failed to create directory")
	}
	namedTableLinks := make([]string, len(store.TTLTables))
	tableNames := make([]string, len(store.TTLTables))
	i := 0
	for _, table := range store.TTLTables {
		tableNames[i] = table.Name
		namedTableLinks[i] = path.Join(tmpDir, storeConfig.Keyspace, table.Name)
		err := os.Symlink(snapshotDir, namedTableLinks[i])
		if err != nil {
			log.WithFields(log.Fields{
				"error": err.Error(),
			}).Panic("failed to create symlink")
		}
		i++
	}

	fmt.Printf("The following tables have been created: %s\n", strings.Join(tableNames, ", "))
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
