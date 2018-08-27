package cassandra

import (
	"flag"
	"time"

	"github.com/rakyll/globalconf"
)

type StoreConfig struct {
	Addrs                    string
	Keyspace                 string
	Consistency              string
	HostSelectionPolicy      string
	Timeout                  time.Duration
	ReadConcurrency          int
	WriteConcurrency         int
	ReadQueueSize            int
	WriteQueueSize           int
	Retries                  int
	WindowFactor             int
	OmitReadTimeout          int
	CqlProtocolVersion       int
	CreateKeyspace           bool
	DisableInitialHostLookup bool
	SSL                      bool
	CaPath                   string
	HostVerification         bool
	Auth                     bool
	Username                 string
	Password                 string
	SchemaFile               string
}

// return StoreConfig with default values set.
func NewStoreConfig() *StoreConfig {
	return &StoreConfig{
		Addrs:                    "localhost",
		Keyspace:                 "metrictank",
		Consistency:              "one",
		HostSelectionPolicy:      "tokenaware,hostpool-epsilon-greedy",
		Timeout:                  time.Second,
		ReadConcurrency:          20,
		WriteConcurrency:         10,
		ReadQueueSize:            200000,
		WriteQueueSize:           100000,
		Retries:                  0,
		WindowFactor:             20,
		OmitReadTimeout:          60,
		CqlProtocolVersion:       4,
		CreateKeyspace:           true,
		DisableInitialHostLookup: false,
		SSL:              false,
		CaPath:           "/etc/metrictank/ca.pem",
		HostVerification: true,
		Auth:             false,
		Username:         "cassandra",
		Password:         "cassandra",
		SchemaFile:       "/etc/metrictank/schema-store-cassandra.toml",
	}
}

var CliConfig = NewStoreConfig()

func ConfigSetup() *flag.FlagSet {
	cas := flag.NewFlagSet("cassandra", flag.ExitOnError)
	cas.StringVar(&CliConfig.Addrs, "addrs", CliConfig.Addrs, "cassandra host (may be given multiple times as comma-separated list)")
	cas.StringVar(&CliConfig.Keyspace, "keyspace", CliConfig.Keyspace, "cassandra keyspace to use for storing the metric data table")
	cas.StringVar(&CliConfig.Consistency, "consistency", CliConfig.Consistency, "write consistency (any|one|two|three|quorum|all|local_quorum|each_quorum|local_one")
	cas.StringVar(&CliConfig.HostSelectionPolicy, "host-selection-policy", CliConfig.HostSelectionPolicy, "")
	cas.DurationVar(&CliConfig.Timeout, "timeout", CliConfig.Timeout, "cassandra timeout")
	cas.IntVar(&CliConfig.ReadConcurrency, "read-concurrency", CliConfig.ReadConcurrency, "max number of concurrent reads to cassandra.")
	cas.IntVar(&CliConfig.WriteConcurrency, "write-concurrency", CliConfig.WriteConcurrency, "max number of concurrent writes to cassandra.")
	cas.IntVar(&CliConfig.ReadQueueSize, "read-queue-size", CliConfig.ReadQueueSize, "max number of outstanding reads before reads will be dropped. This is important if you run queries that result in many reads in parallel.")
	cas.IntVar(&CliConfig.WriteQueueSize, "write-queue-size", CliConfig.WriteQueueSize, "write queue size per cassandra worker. should be large engough to hold all at least the total number of series expected, divided by how many workers you have")
	cas.IntVar(&CliConfig.Retries, "retries", CliConfig.Retries, "how many times to retry a query before failing it")
	cas.IntVar(&CliConfig.WindowFactor, "window-factor", CliConfig.WindowFactor, "size of compaction window relative to TTL")
	cas.IntVar(&CliConfig.OmitReadTimeout, "omit-read-timeout", CliConfig.OmitReadTimeout, "if a read is older than this (in seconds), it will be omitted,  not executed")
	cas.IntVar(&CliConfig.CqlProtocolVersion, "cql-protocol-version", CliConfig.CqlProtocolVersion, "cql protocol version to use")
	cas.BoolVar(&CliConfig.CreateKeyspace, "create-keyspace", CliConfig.CreateKeyspace, "enable the creation of the mdata keyspace and tables, only one node needs this")
	cas.BoolVar(&CliConfig.DisableInitialHostLookup, "disable-initial-host-lookup", CliConfig.DisableInitialHostLookup, "instruct the driver to not attempt to get host info from the system.peers table")
	cas.BoolVar(&CliConfig.SSL, "ssl", CliConfig.SSL, "enable SSL connection to cassandra")
	cas.StringVar(&CliConfig.CaPath, "ca-path", CliConfig.CaPath, "cassandra CA certificate path when using SSL")
	cas.BoolVar(&CliConfig.HostVerification, "host-verification", CliConfig.HostVerification, "host (hostname and server cert) verification when using SSL")
	cas.BoolVar(&CliConfig.Auth, "auth", CliConfig.Auth, "enable cassandra authentication")
	cas.StringVar(&CliConfig.Username, "username", CliConfig.Username, "username for authentication")
	cas.StringVar(&CliConfig.Password, "password", CliConfig.Password, "password for authentication")
	cas.StringVar(&CliConfig.SchemaFile, "schema-file", CliConfig.SchemaFile, "File containing the needed schemas in case database needs initializing")
	globalconf.Register("cassandra", cas)
	return cas
}
