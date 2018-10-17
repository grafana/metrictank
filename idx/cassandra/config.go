package cassandra

import (
	"flag"

	"github.com/rakyll/globalconf"
)

// IdxConfig holds all configuration variables for a Cassandra Index
type IdxConfig struct {
	Enabled                  bool
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
	timeoutStr               string
	timeout                  uint32 // in seconds
	numConns                 int
	writeQueueSize           int
	protoVer                 int
	maxStaleStr              string
	maxStale                 uint32 // in seconds
	pruneIntervalStr         string
	pruneInterval            uint32 // in seconds
	updateCassIdx            bool
	updateIntervalStr        string
	updateInterval           uint32 // in seconds
	disableInitialHostLookup bool
}

// NewCasIdxConfig returns a new IdxConfig with default values set
func NewCasIdxConfig() *IdxConfig {
	return &IdxConfig{
		Enabled:                  true,
		hosts:                    "localhost:9042",
		keyspace:                 "metrictank",
		consistency:              "one",
		timeoutStr:               "1s",
		numConns:                 10,
		writeQueueSize:           100000,
		updateCassIdx:            true,
		updateIntervalStr:        "3h",
		maxStaleStr:              "0s",
		pruneIntervalStr:         "3h",
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

// CasIdxConfig stores a Cassandra Index configuration with default values
var CasIdxConfig = NewCasIdxConfig()

// ConfigSetup creates a Cassandra Index configuration using default values, values passed in through the command line, or values from the environment
func ConfigSetup() *flag.FlagSet {
	casIdx := flag.NewFlagSet("cassandra-idx", flag.ExitOnError)

	casIdx.BoolVar(&CasIdxConfig.Enabled, "enabled", CasIdxConfig.Enabled, "")
	casIdx.StringVar(&CasIdxConfig.hosts, "hosts", CasIdxConfig.hosts, "comma separated list of cassandra addresses in host:port form")
	casIdx.StringVar(&CasIdxConfig.keyspace, "keyspace", CasIdxConfig.keyspace, "Cassandra keyspace to store metricDefinitions in.")
	casIdx.StringVar(&CasIdxConfig.consistency, "consistency", CasIdxConfig.consistency, "write consistency (any|one|two|three|quorum|all|local_quorum|each_quorum|local_one")
	casIdx.StringVar(&CasIdxConfig.timeoutStr, "timeout", CasIdxConfig.timeoutStr, "cassandra request timeout")
	casIdx.IntVar(&CasIdxConfig.numConns, "num-conns", CasIdxConfig.numConns, "number of concurrent connections to cassandra")
	casIdx.IntVar(&CasIdxConfig.writeQueueSize, "write-queue-size", CasIdxConfig.writeQueueSize, "Max number of metricDefs allowed to be unwritten to cassandra")
	casIdx.BoolVar(&CasIdxConfig.updateCassIdx, "update-cassandra-index", CasIdxConfig.updateCassIdx, "synchronize index changes to cassandra. not all your nodes need to do this.")
	casIdx.StringVar(&CasIdxConfig.updateIntervalStr, "update-interval", CasIdxConfig.updateIntervalStr, "frequency at which we should update the metricDef lastUpdate field, use 0s for instant updates")
	casIdx.StringVar(&CasIdxConfig.maxStaleStr, "max-stale", CasIdxConfig.maxStaleStr, "clear series from the index if they have not been seen for this much time.")
	casIdx.StringVar(&CasIdxConfig.pruneIntervalStr, "prune-interval", CasIdxConfig.pruneIntervalStr, "Interval at which the index should be checked for stale series.")
	casIdx.IntVar(&CasIdxConfig.protoVer, "protocol-version", CasIdxConfig.protoVer, "cql protocol version to use")
	casIdx.BoolVar(&CasIdxConfig.createKeyspace, "create-keyspace", CasIdxConfig.createKeyspace, "enable the creation of the index keyspace and tables, only one node needs this")
	casIdx.StringVar(&CasIdxConfig.schemaFile, "schema-file", CasIdxConfig.schemaFile, "File containing the needed schemas in case database needs initializing")
	casIdx.BoolVar(&CasIdxConfig.disableInitialHostLookup, "disable-initial-host-lookup", CasIdxConfig.disableInitialHostLookup, "instruct the driver to not attempt to get host info from the system.peers table")
	casIdx.BoolVar(&CasIdxConfig.ssl, "ssl", CasIdxConfig.ssl, "enable SSL connection to cassandra")
	casIdx.StringVar(&CasIdxConfig.capath, "ca-path", CasIdxConfig.capath, "cassandra CA certficate path when using SSL")
	casIdx.BoolVar(&CasIdxConfig.hostverification, "host-verification", CasIdxConfig.hostverification, "host (hostname and server cert) verification when using SSL")

	casIdx.BoolVar(&CasIdxConfig.auth, "auth", CasIdxConfig.auth, "enable cassandra user authentication")
	casIdx.StringVar(&CasIdxConfig.username, "username", CasIdxConfig.username, "username for authentication")
	casIdx.StringVar(&CasIdxConfig.password, "password", CasIdxConfig.password, "password for authentication")

	globalconf.Register("cassandra-idx", casIdx)
	return casIdx
}
