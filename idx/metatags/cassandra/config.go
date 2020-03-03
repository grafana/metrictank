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
var CliConfig = NewConfig()

// Config stores configuration settings for a cassandra meta record index
type Config struct {
	Enabled                  bool
	updateCassIdx            bool
	ssl                      bool
	auth                     bool
	hostVerification         bool
	createKeyspace           bool
	schemaFile               string
	keyspace                 string
	metaRecordTable          string
	metaRecordBatchTable     string
	metaRecordPollInterval   time.Duration
	metaRecordPruneInterval  time.Duration
	metaRecordPruneAge       time.Duration
	hosts                    string
	caPath                   string
	username                 string
	password                 string
	consistency              string
	timeout                  time.Duration
	connectionCheckInterval  time.Duration
	connectionCheckTimeout   time.Duration
	numConns                 int
	protoVer                 int
	disableInitialHostLookup bool
}

// NewConfig returns MetaRecordIdxConfig with default values set.
func NewConfig() *Config {
	return &Config{
		Enabled:                  true,
		hosts:                    "localhost:9042",
		keyspace:                 "metrictank",
		metaRecordTable:          "meta_records",
		metaRecordBatchTable:     "meta_record_batches",
		metaRecordPollInterval:   time.Second * 10,
		metaRecordPruneInterval:  time.Hour * 24,
		metaRecordPruneAge:       time.Hour * 72,
		consistency:              "one",
		timeout:                  time.Second,
		connectionCheckInterval:  time.Second * 5,
		connectionCheckTimeout:   time.Second * 30,
		numConns:                 10,
		updateCassIdx:            true,
		protoVer:                 4,
		createKeyspace:           true,
		schemaFile:               "/etc/metrictank/schema-idx-cassandra.toml",
		disableInitialHostLookup: false,
		ssl:                      false,
		caPath:                   "/etc/metrictank/ca.pem",
		hostVerification:         true,
		auth:                     false,
		username:                 "cassandra",
		password:                 "cassandra",
	}
}

// Validate validates MetaRecordIdxConfig settings
func (cfg *Config) Validate() error {
	if cfg.timeout == 0 {
		return errors.New("timeout must be greater than 0. " + timeUnits)
	}
	return nil
}

// ConfigSetup sets up and registers a FlagSet in globalconf for cassandra index and returns it
func ConfigSetup() *flag.FlagSet {
	config := flag.NewFlagSet("cassandra-meta-record-idx", flag.ExitOnError)

	config.BoolVar(&CliConfig.Enabled, "enabled", CliConfig.Enabled, "")
	config.StringVar(&CliConfig.hosts, "hosts", CliConfig.hosts, "comma separated list of cassandra addresses in host:port form")
	config.StringVar(&CliConfig.keyspace, "keyspace", CliConfig.keyspace, "Cassandra keyspace to store metricDefinitions in.")
	config.StringVar(&CliConfig.metaRecordTable, "meta-record-table", CliConfig.metaRecordTable, "Cassandra table to store meta records.")
	config.StringVar(&CliConfig.metaRecordBatchTable, "meta-record-batch-table", CliConfig.metaRecordBatchTable, "Cassandra table to store meta data of meta record batches.")
	config.DurationVar(&CliConfig.metaRecordPollInterval, "meta-record-poll-interval", CliConfig.metaRecordPollInterval, "Interval at which to poll store for meta record updates.")
	config.DurationVar(&CliConfig.metaRecordPruneInterval, "meta-record-prune-interval", CliConfig.metaRecordPruneInterval, "Interval at which meta records of old batches get pruned.")
	config.DurationVar(&CliConfig.metaRecordPruneAge, "meta-record-prune-age", CliConfig.metaRecordPruneAge, "The minimum age a batch of meta records must have to be pruned.")
	config.StringVar(&CliConfig.consistency, "consistency", CliConfig.consistency, "write consistency (any|one|two|three|quorum|all|local_quorum|each_quorum|local_one")
	config.DurationVar(&CliConfig.timeout, "timeout", CliConfig.timeout, "cassandra request timeout")
	config.DurationVar(&CliConfig.connectionCheckInterval, "connection-check-interval", CliConfig.connectionCheckInterval, "interval at which to perform a connection check to cassandra, set to 0 to disable.")
	config.DurationVar(&CliConfig.connectionCheckTimeout, "connection-check-timeout", CliConfig.connectionCheckTimeout, "maximum total time to wait before considering a connection to cassandra invalid. This value should be higher than connection-check-interval.")
	config.IntVar(&CliConfig.numConns, "num-conns", CliConfig.numConns, "number of concurrent connections to cassandra")
	config.BoolVar(&CliConfig.updateCassIdx, "update-cassandra-index", CliConfig.updateCassIdx, "synchronize index changes to cassandra. not all your nodes need to do this.")
	config.IntVar(&CliConfig.protoVer, "protocol-version", CliConfig.protoVer, "cql protocol version to use")
	config.BoolVar(&CliConfig.createKeyspace, "create-keyspace", CliConfig.createKeyspace, "enable the creation of the index keyspace and tables, only one node needs this")
	config.StringVar(&CliConfig.schemaFile, "schema-file", CliConfig.schemaFile, "File containing the needed schemas in case database needs initializing")
	config.BoolVar(&CliConfig.disableInitialHostLookup, "disable-initial-host-lookup", CliConfig.disableInitialHostLookup, "instruct the driver to not attempt to get host info from the system.peers table")
	config.BoolVar(&CliConfig.ssl, "ssl", CliConfig.ssl, "enable SSL connection to cassandra")
	config.StringVar(&CliConfig.caPath, "ca-path", CliConfig.caPath, "cassandra CA certficate path when using SSL")
	config.BoolVar(&CliConfig.hostVerification, "host-verification", CliConfig.hostVerification, "host (hostname and server cert) verification when using SSL")
	config.BoolVar(&CliConfig.auth, "auth", CliConfig.auth, "enable cassandra user authentication")
	config.StringVar(&CliConfig.username, "username", CliConfig.username, "username for authentication")
	config.StringVar(&CliConfig.password, "password", CliConfig.password, "password for authentication")

	globalconf.Register("cassandra-meta-record-idx", config, flag.ExitOnError)
	return config
}

// ConfigProcess calls MetaRecordIdxConfig.Validate() on CliConfig. If an error is discovered this will exit with status set to 1.
func ConfigProcess() {
	if err := CliConfig.Validate(); err != nil {
		log.Fatalf("cassandra-meta-record-idx: Config validation error. %s", err)
	}
}
