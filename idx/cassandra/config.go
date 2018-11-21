package cassandra

import (
	"errors"
	"flag"
	"time"

	"github.com/rakyll/globalconf"
	log "github.com/sirupsen/logrus"
)

// time units accepted by time.ParseDuration
const timeUnits = "Valid time units are 'ns', 'us' (or 'µs'), 'ms', 's', 'm', 'h'"

// CliConfig is a cassandra IdxConfig. It is instantiated with default values which can then be changed.
var CliConfig = NewIdxConfig()

// IdxConfig stores configuration settings for a cassandra index
type IdxConfig struct {
	Enabled          bool
	pruneInterval    time.Duration
	updateCassIdx    bool
	updateInterval   time.Duration
	updateInterval32 uint32

	writeQueueSize int

	ssl                      bool
	auth                     bool
	hostverification         bool
	createKeyspace           bool
	schemaFile               string
	keyspace                 string
	hosts                    string
	capath                   string
	username                 string
	password                 string
	consistency              string
	timeout                  time.Duration
	numConns                 int
	protoVer                 int
	disableInitialHostLookup bool
}

// NewIdxConfig returns IdxConfig with default values set.
func NewIdxConfig() *IdxConfig {
	return &IdxConfig{
		Enabled:                  true,
		hosts:                    "localhost:9042",
		keyspace:                 "metrictank",
		consistency:              "one",
		timeout:                  time.Second,
		numConns:                 10,
		writeQueueSize:           100000,
		updateCassIdx:            true,
		updateInterval:           time.Hour * 3,
		pruneInterval:            time.Hour * 3,
		protoVer:                 4,
		createKeyspace:           true,
		schemaFile:               "/etc/metrictank/schema-idx-cassandra.toml",
		disableInitialHostLookup: false,
		ssl:                      false,
		capath:                   "/etc/metrictank/ca.pem",
		hostverification:         true,
		auth:                     false,
		username:                 "cassandra",
		password:                 "cassandra",
	}
}

// Validate validates IdxConfig settings
func (cfg *IdxConfig) Validate() error {
	if cfg.updateInterval == 0 {
		return errors.New("updateInterval must be greater than 0. " + timeUnits)
	}
	if cfg.pruneInterval == 0 {
		return errors.New("pruneInterval must be greater then 0. " + timeUnits)
	}
	if cfg.timeout == 0 {
		return errors.New("timeout must be greater than 0. " + timeUnits)
	}
	return nil
}

// ConfigSetup sets up and registers a FlagSet in globalconf for cassandra index and returns it
func ConfigSetup() *flag.FlagSet {
	casIdx := flag.NewFlagSet("cassandra-idx", flag.ExitOnError)

	casIdx.BoolVar(&CliConfig.Enabled, "enabled", CliConfig.Enabled, "")
	casIdx.StringVar(&CliConfig.hosts, "hosts", CliConfig.hosts, "comma separated list of cassandra addresses in host:port form")
	casIdx.StringVar(&CliConfig.keyspace, "keyspace", CliConfig.keyspace, "Cassandra keyspace to store metricDefinitions in.")
	casIdx.StringVar(&CliConfig.consistency, "consistency", CliConfig.consistency, "write consistency (any|one|two|three|quorum|all|local_quorum|each_quorum|local_one")
	casIdx.DurationVar(&CliConfig.timeout, "timeout", CliConfig.timeout, "cassandra request timeout")
	casIdx.IntVar(&CliConfig.numConns, "num-conns", CliConfig.numConns, "number of concurrent connections to cassandra")
	casIdx.IntVar(&CliConfig.writeQueueSize, "write-queue-size", CliConfig.writeQueueSize, "Max number of metricDefs allowed to be unwritten to cassandra")
	casIdx.BoolVar(&CliConfig.updateCassIdx, "update-cassandra-index", CliConfig.updateCassIdx, "synchronize index changes to cassandra. not all your nodes need to do this.")
	casIdx.DurationVar(&CliConfig.updateInterval, "update-interval", CliConfig.updateInterval, "frequency at which we should update the metricDef lastUpdate field, use 0s for instant updates")
	casIdx.DurationVar(&CliConfig.pruneInterval, "prune-interval", CliConfig.pruneInterval, "Interval at which the index should be checked for stale series.")
	casIdx.IntVar(&CliConfig.protoVer, "protocol-version", CliConfig.protoVer, "cql protocol version to use")
	casIdx.BoolVar(&CliConfig.createKeyspace, "create-keyspace", CliConfig.createKeyspace, "enable the creation of the index keyspace and tables, only one node needs this")
	casIdx.StringVar(&CliConfig.schemaFile, "schema-file", CliConfig.schemaFile, "File containing the needed schemas in case database needs initializing")
	casIdx.BoolVar(&CliConfig.disableInitialHostLookup, "disable-initial-host-lookup", CliConfig.disableInitialHostLookup, "instruct the driver to not attempt to get host info from the system.peers table")
	casIdx.BoolVar(&CliConfig.ssl, "ssl", CliConfig.ssl, "enable SSL connection to cassandra")
	casIdx.StringVar(&CliConfig.capath, "ca-path", CliConfig.capath, "cassandra CA certficate path when using SSL")
	casIdx.BoolVar(&CliConfig.hostverification, "host-verification", CliConfig.hostverification, "host (hostname and server cert) verification when using SSL")

	casIdx.BoolVar(&CliConfig.auth, "auth", CliConfig.auth, "enable cassandra user authentication")
	casIdx.StringVar(&CliConfig.username, "username", CliConfig.username, "username for authentication")
	casIdx.StringVar(&CliConfig.password, "password", CliConfig.password, "password for authentication")

	globalconf.Register("cassandra-idx", casIdx)
	return casIdx
}

// ConfigProcess calls IdxConfig.Validate() on CliConfig. If an error is discovered this will exist with status set to 1.
func ConfigProcess() {
	if err := CliConfig.Validate(); err != nil {
		log.Fatalf("cassandra-idx: Config validation error. %s", err)
	}
}
