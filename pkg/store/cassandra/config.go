package cassandra

import (
	"errors"
	"flag"
	"fmt"
	"time"

	"github.com/grafana/metrictank/mdata/chunk"

	"github.com/grafana/globalconf"
)

type StoreConfig struct {
	Enabled                  bool
	Addrs                    string
	Keyspace                 string
	Consistency              string
	HostSelectionPolicy      string
	Timeout                  string
	ReadConcurrency          int
	WriteConcurrency         int
	ReadQueueSize            int
	WriteQueueSize           int
	Retries                  int
	WindowFactor             int
	OmitReadTimeout          string
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
	ConnectionCheckInterval  time.Duration
	ConnectionCheckTimeout   time.Duration
	MaxChunkSpan             time.Duration
}

// Validate makes sure the StoreConfig settings are valid
func (cfg *StoreConfig) Validate(schemaMaxChunkSpan uint32) error {
	if CliConfig.MaxChunkSpan%time.Second != 0 {
		return errors.New("max-chunkspan must be a whole number of seconds")
	}
	if CliConfig.Enabled {
		if uint32(CliConfig.MaxChunkSpan.Seconds()) < schemaMaxChunkSpan {
			return fmt.Errorf("max-chunkspan must be at least as high as the max chunkspan used in your storage-schemas (which is %d)", schemaMaxChunkSpan)
		}
	}
	return nil
}

// return StoreConfig with default values set.
func NewStoreConfig() *StoreConfig {
	return &StoreConfig{
		Enabled:                  true,
		Addrs:                    "localhost",
		Keyspace:                 "metrictank",
		Consistency:              "one",
		HostSelectionPolicy:      "tokenaware,hostpool-epsilon-greedy",
		Timeout:                  "1s",
		ReadConcurrency:          20,
		WriteConcurrency:         10,
		ReadQueueSize:            200000,
		WriteQueueSize:           100000,
		Retries:                  0,
		WindowFactor:             20,
		OmitReadTimeout:          "60s",
		CqlProtocolVersion:       4,
		CreateKeyspace:           true,
		DisableInitialHostLookup: false,
		SSL:                      false,
		CaPath:                   "/etc/metrictank/ca.pem",
		HostVerification:         true,
		Auth:                     false,
		Username:                 "cassandra",
		Password:                 "cassandra",
		SchemaFile:               "/etc/metrictank/schema-store-cassandra.toml",
		ConnectionCheckInterval:  time.Second * 5,
		ConnectionCheckTimeout:   time.Second * 30,
		MaxChunkSpan:             time.Second * time.Duration(chunk.MaxConfigurableSpan()),
	}
}

var CliConfig = NewStoreConfig()

func ConfigSetup() *flag.FlagSet {
	cas := flag.NewFlagSet("cassandra", flag.ExitOnError)
	cas.BoolVar(&CliConfig.Enabled, "enabled", CliConfig.Enabled, "enable the cassandra backend store plugin")
	cas.StringVar(&CliConfig.Addrs, "addrs", CliConfig.Addrs, "cassandra host (may be given multiple times as comma-separated list)")
	cas.StringVar(&CliConfig.Keyspace, "keyspace", CliConfig.Keyspace, "cassandra keyspace to use for storing the metric data table")
	cas.StringVar(&CliConfig.Consistency, "consistency", CliConfig.Consistency, "write consistency (any|one|two|three|quorum|all|local_quorum|each_quorum|local_one")
	cas.StringVar(&CliConfig.HostSelectionPolicy, "host-selection-policy", CliConfig.HostSelectionPolicy, "")
	cas.StringVar(&CliConfig.Timeout, "timeout", CliConfig.Timeout, "cassandra timeout")
	cas.IntVar(&CliConfig.ReadConcurrency, "read-concurrency", CliConfig.ReadConcurrency, "max number of concurrent reads to cassandra.")
	cas.IntVar(&CliConfig.WriteConcurrency, "write-concurrency", CliConfig.WriteConcurrency, "max number of concurrent writes to cassandra.")
	cas.IntVar(&CliConfig.ReadQueueSize, "read-queue-size", CliConfig.ReadQueueSize, "max number of outstanding reads before reads will be dropped. This is important if you run queries that result in many reads in parallel.")
	cas.IntVar(&CliConfig.WriteQueueSize, "write-queue-size", CliConfig.WriteQueueSize, "write queue size per cassandra worker. should be large engough to hold all at least the total number of series expected, divided by how many workers you have")
	cas.IntVar(&CliConfig.Retries, "retries", CliConfig.Retries, "how many times to retry a query before failing it")
	cas.IntVar(&CliConfig.WindowFactor, "window-factor", CliConfig.WindowFactor, "size of compaction window relative to TTL")
	cas.StringVar(&CliConfig.OmitReadTimeout, "omit-read-timeout", CliConfig.OmitReadTimeout, "if a read is older than this, it will be omitted,  not executed")
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
	cas.DurationVar(&CliConfig.ConnectionCheckInterval, "connection-check-interval", CliConfig.ConnectionCheckInterval, "interval at which to perform a connection check to cassandra, set to 0 to disable.")
	cas.DurationVar(&CliConfig.ConnectionCheckTimeout, "connection-check-timeout", CliConfig.ConnectionCheckTimeout, "maximum total time to wait before considering a connection to cassandra invalid. This value should be higher than connection-check-interval.")
	cas.DurationVar(&CliConfig.MaxChunkSpan, "max-chunkspan", CliConfig.MaxChunkSpan, "Maximum chunkspan size used.")
	globalconf.Register("cassandra", cas, flag.ExitOnError)
	return cas
}
