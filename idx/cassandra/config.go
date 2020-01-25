package cassandra

import (
	"errors"
	"flag"
	"time"

	"github.com/grafana/globalconf"
	log "github.com/sirupsen/logrus"
)

// time units accepted by time.ParseDuration
const timeUnits = "Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h'"

// CliConfig is a cassandra IdxConfig. It is instantiated with default values which can then be changed.
var CliConfig = NewIdxConfig()

// IdxConfig stores configuration settings for a cassandra index
type IdxConfig struct {
	Enabled        bool
	pruneInterval  time.Duration
	updateCassIdx  bool
	updateInterval time.Duration

	writeQueueSize int

	SSL                      bool
	Auth                     bool
	HostVerification         bool
	CreateKeyspace           bool
	SchemaFile               string
	Keyspace                 string
	Table                    string
	ArchiveTable             string
	Hosts                    string
	CaPath                   string
	Username                 string
	Password                 string
	Consistency              string
	Timeout                  time.Duration
	ConnectionCheckInterval  time.Duration
	ConnectionCheckTimeout   time.Duration
	NumConns                 int
	ProtoVer                 int
	DisableInitialHostLookup bool
	InitLoadConcurrency      int
}

// NewIdxConfig returns IdxConfig with default values set.
func NewIdxConfig() *IdxConfig {
	return &IdxConfig{
		Enabled:                  true,
		Hosts:                    "localhost:9042",
		Keyspace:                 "metrictank",
		Table:                    "metric_idx",
		ArchiveTable:             "metric_idx_archive",
		Consistency:              "one",
		Timeout:                  time.Second,
		ConnectionCheckInterval:  time.Second * 5,
		ConnectionCheckTimeout:   time.Second * 30,
		NumConns:                 10,
		writeQueueSize:           100000,
		updateCassIdx:            true,
		updateInterval:           time.Hour * 3,
		pruneInterval:            time.Hour * 3,
		ProtoVer:                 4,
		CreateKeyspace:           true,
		SchemaFile:               "/etc/metrictank/schema-idx-cassandra.toml",
		DisableInitialHostLookup: false,
		SSL:                      false,
		CaPath:                   "/etc/metrictank/ca.pem",
		HostVerification:         true,
		Auth:                     false,
		Username:                 "cassandra",
		Password:                 "cassandra",
		InitLoadConcurrency:      1,
	}
}

// Validate validates IdxConfig settings
func (cfg *IdxConfig) Validate() error {
	if cfg.pruneInterval == 0 {
		return errors.New("pruneInterval must be greater then 0. " + timeUnits)
	}
	if cfg.Timeout == 0 {
		return errors.New("timeout must be greater than 0. " + timeUnits)
	}
	return nil
}

// ConfigSetup sets up and registers a FlagSet in globalconf for cassandra index and returns it
func ConfigSetup() *flag.FlagSet {
	casIdx := flag.NewFlagSet("cassandra-idx", flag.ExitOnError)

	casIdx.BoolVar(&CliConfig.Enabled, "enabled", CliConfig.Enabled, "")
	casIdx.StringVar(&CliConfig.Hosts, "hosts", CliConfig.Hosts, "comma separated list of cassandra addresses in host:port form")
	casIdx.StringVar(&CliConfig.Keyspace, "keyspace", CliConfig.Keyspace, "Cassandra keyspace to store metricDefinitions in.")
	casIdx.StringVar(&CliConfig.Table, "table", CliConfig.Table, "Cassandra table to store metricDefinitions in.")
	casIdx.StringVar(&CliConfig.ArchiveTable, "archive-table", CliConfig.ArchiveTable, "Cassandra table to archive metricDefinitions in.")
	casIdx.StringVar(&CliConfig.Consistency, "consistency", CliConfig.Consistency, "write consistency (any|one|two|three|quorum|all|local_quorum|each_quorum|local_one")
	casIdx.DurationVar(&CliConfig.Timeout, "timeout", CliConfig.Timeout, "cassandra request timeout")
	casIdx.DurationVar(&CliConfig.ConnectionCheckInterval, "connection-check-interval", CliConfig.ConnectionCheckInterval, "interval at which to perform a connection check to cassandra, set to 0 to disable.")
	casIdx.DurationVar(&CliConfig.ConnectionCheckTimeout, "connection-check-timeout", CliConfig.ConnectionCheckTimeout, "maximum total time to wait before considering a connection to cassandra invalid. This value should be higher than connection-check-interval.")
	casIdx.IntVar(&CliConfig.NumConns, "num-conns", CliConfig.NumConns, "number of concurrent connections to cassandra")
	casIdx.IntVar(&CliConfig.writeQueueSize, "write-queue-size", CliConfig.writeQueueSize, "Max number of metricDefs allowed to be unwritten to cassandra")
	casIdx.BoolVar(&CliConfig.updateCassIdx, "update-cassandra-index", CliConfig.updateCassIdx, "synchronize index changes to cassandra. not all your nodes need to do this.")
	casIdx.DurationVar(&CliConfig.updateInterval, "update-interval", CliConfig.updateInterval, "frequency at which we should update the metricDef lastUpdate field, use 0s for instant updates")
	casIdx.DurationVar(&CliConfig.pruneInterval, "prune-interval", CliConfig.pruneInterval, "Interval at which the index should be checked for stale series.")
	casIdx.IntVar(&CliConfig.InitLoadConcurrency, "init-load-concurrency", CliConfig.InitLoadConcurrency, "Number of partitions to load concurrently on startup.")
	casIdx.IntVar(&CliConfig.ProtoVer, "protocol-version", CliConfig.ProtoVer, "cql protocol version to use")
	casIdx.BoolVar(&CliConfig.CreateKeyspace, "create-keyspace", CliConfig.CreateKeyspace, "enable the creation of the index keyspace and tables, only one node needs this")
	casIdx.StringVar(&CliConfig.SchemaFile, "schema-file", CliConfig.SchemaFile, "File containing the needed schemas in case database needs initializing")
	casIdx.BoolVar(&CliConfig.DisableInitialHostLookup, "disable-initial-host-lookup", CliConfig.DisableInitialHostLookup, "instruct the driver to not attempt to get host info from the system.peers table")
	casIdx.BoolVar(&CliConfig.SSL, "ssl", CliConfig.SSL, "enable SSL connection to cassandra")
	casIdx.StringVar(&CliConfig.CaPath, "ca-path", CliConfig.CaPath, "cassandra CA certficate path when using SSL")
	casIdx.BoolVar(&CliConfig.HostVerification, "host-verification", CliConfig.HostVerification, "host (hostname and server cert) verification when using SSL")

	casIdx.BoolVar(&CliConfig.Auth, "auth", CliConfig.Auth, "enable cassandra user authentication")
	casIdx.StringVar(&CliConfig.Username, "username", CliConfig.Username, "username for authentication")
	casIdx.StringVar(&CliConfig.Password, "password", CliConfig.Password, "password for authentication")

	globalconf.Register("cassandra-idx", casIdx, flag.ExitOnError)
	return casIdx
}

// ConfigProcess calls IdxConfig.Validate() on CliConfig. If an error is discovered this will exit with status set to 1.
func ConfigProcess() {
	if err := CliConfig.Validate(); err != nil {
		log.Fatalf("cassandra-idx: Config validation error. %s", err)
	}
}
