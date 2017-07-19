package main

import (
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/gocql/gocql"
	hostpool "github.com/hailocab/go-hostpool"
	"github.com/raintank/dur"
)

var (
	cassandraAddrs               = flag.String("cassandra-addrs", "localhost", "cassandra host (may be given multiple times as comma-separated list)")
	cassandraKeyspace            = flag.String("cassandra-keyspace", "metrictank", "cassandra keyspace to use for storing the metric data table")
	cassandraConsistency         = flag.String("cassandra-consistency", "one", "write consistency (any|one|two|three|quorum|all|local_quorum|each_quorum|local_one")
	cassandraHostSelectionPolicy = flag.String("cassandra-host-selection-policy", "tokenaware,hostpool-epsilon-greedy", "")
	cassandraTimeout             = flag.Int("cassandra-timeout", 1000, "cassandra timeout in milliseconds")
	cassandraConcurrency         = flag.Int("cassandra-concurrency", 20, "max number of concurrent reads to cassandra.")
	cassandraRetries             = flag.Int("cassandra-retries", 0, "how many times to retry a query before failing it")
	cqlProtocolVersion           = flag.Int("cql-protocol-version", 4, "cql protocol version to use")

	cassandraSSL              = flag.Bool("cassandra-ssl", false, "enable SSL connection to cassandra")
	cassandraCaPath           = flag.String("cassandra-ca-path", "/etc/metrictank/ca.pem", "cassandra CA certificate path when using SSL")
	cassandraHostVerification = flag.Bool("cassandra-host-verification", true, "host (hostname and server cert) verification when using SSL")

	cassandraAuth     = flag.Bool("cassandra-auth", false, "enable cassandra authentication")
	cassandraUsername = flag.String("cassandra-username", "cassandra", "username for authentication")
	cassandraPassword = flag.String("cassandra-password", "cassandra", "password for authentication")

	verbose = flag.Bool("verbose", false, "show every record being processed")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "mt-update-ttl [flags] ttl table-in [table-out]")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Adjusts the data in Cassandra to use a new TTL value. The TTL is applied counting from the timestamp of the data")
		fmt.Fprintln(os.Stderr, "If table-out not specified or same as table-in, will update in place. Otherwise will not touch input table and store results in table-out")
		fmt.Fprintln(os.Stderr, "In that case, it is up to you to assure table-out exists before running this tool")
		fmt.Fprintln(os.Stderr, "Not supported yet: for the per-ttl tables as of 0.7, automatically putting data in the right table")
		fmt.Println("Flags:")
		flag.PrintDefaults()
		os.Exit(-1)
	}
	flag.Parse()

	if flag.NArg() < 2 {
		flag.Usage()
	}

	ttl := int(dur.MustParseNDuration("ttl", flag.Arg(0)))
	tableIn, tableOut := flag.Arg(1), flag.Arg(1)
	if flag.NArg() == 3 {
		tableOut = flag.Arg(2)
	}

	session, err := NewCassandraStore()

	if err != nil {
		panic(fmt.Sprintf("Failed to instantiate cassandra: %s", err))
	}

	update(session, ttl, tableIn, tableOut)
}

func NewCassandraStore() (*gocql.Session, error) {
	cluster := gocql.NewCluster(strings.Split(*cassandraAddrs, ",")...)
	if *cassandraSSL {
		cluster.SslOpts = &gocql.SslOptions{
			CaPath:                 *cassandraCaPath,
			EnableHostVerification: *cassandraHostVerification,
		}
	}
	if *cassandraAuth {
		cluster.Authenticator = gocql.PasswordAuthenticator{
			Username: *cassandraUsername,
			Password: *cassandraPassword,
		}
	}
	cluster.Consistency = gocql.ParseConsistency(*cassandraConsistency)
	cluster.Timeout = time.Duration(*cassandraTimeout) * time.Millisecond
	cluster.NumConns = *cassandraConcurrency
	cluster.ProtoVersion = *cqlProtocolVersion
	cluster.Keyspace = *cassandraKeyspace
	cluster.RetryPolicy = &gocql.SimpleRetryPolicy{NumRetries: *cassandraRetries}

	switch *cassandraHostSelectionPolicy {
	case "roundrobin":
		cluster.PoolConfig.HostSelectionPolicy = gocql.RoundRobinHostPolicy()
	case "hostpool-simple":
		cluster.PoolConfig.HostSelectionPolicy = gocql.HostPoolHostPolicy(hostpool.New(nil))
	case "hostpool-epsilon-greedy":
		cluster.PoolConfig.HostSelectionPolicy = gocql.HostPoolHostPolicy(
			hostpool.NewEpsilonGreedy(nil, 0, &hostpool.LinearEpsilonValueCalculator{}),
		)
	case "tokenaware,roundrobin":
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(
			gocql.RoundRobinHostPolicy(),
		)
	case "tokenaware,hostpool-simple":
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(
			gocql.HostPoolHostPolicy(hostpool.New(nil)),
		)
	case "tokenaware,hostpool-epsilon-greedy":
		cluster.PoolConfig.HostSelectionPolicy = gocql.TokenAwareHostPolicy(
			gocql.HostPoolHostPolicy(
				hostpool.NewEpsilonGreedy(nil, 0, &hostpool.LinearEpsilonValueCalculator{}),
			),
		)
	default:
		return nil, fmt.Errorf("unknown HostSelectionPolicy '%q'", *cassandraHostSelectionPolicy)
	}

	return cluster.CreateSession()
}

func getTTL(now, ts, ttl int) int {
	newTTL := ttl - (now - ts)

	// setting a TTL of 0 or negative, is equivalent to no TTL = keep forever.
	// we wouldn't want that to happen by accident.
	// this can happen when setting or shortening a TTL for already very old data.
	if newTTL < 1 {
		return 1
	}
	return newTTL
}

func update(session *gocql.Session, ttl int, tableIn, tableOut string) {
	var key string
	var ts int
	var data []byte
	var query string
	iter := session.Query(fmt.Sprintf("SELECT key, ts, data FROM %s", tableIn)).Iter()
	rownum := 0
	for iter.Scan(&key, &ts, &data) {
		newTTL := getTTL(int(time.Now().Unix()), ts, ttl)
		if tableIn == tableOut {
			query = fmt.Sprintf("UPDATE %s USING TTL %d SET data = ? WHERE key = ? AND ts = ?", tableIn, newTTL)
		} else {
			query = fmt.Sprintf("INSERT INTO %s (data, key, ts) values(?,?,?) USING TTL %d", tableOut, newTTL)
		}
		if *verbose {
			fmt.Printf("processing rownum=%d table=%q key=%q ts=%d data='%x'\n", rownum, tableIn, key, ts, data)
			fmt.Println("Query:", query)

		}

		err := session.Query(query, data, key, ts).Exec()
		if err != nil {
			fmt.Fprintf(os.Stderr, "ERROR: failed updating %s %s %d: %q", tableOut, key, ts, err)
			iter.Close()
			os.Exit(2)
		}
		rownum++
	}
	err := iter.Close()
	if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: failed querying %s: %q. processed %d rows", tableIn, err, rownum)
		os.Exit(2)
	}
	fmt.Println("processed", rownum, "rows")
}
