package cassandra

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/grafana/metrictank/idx/memory"

	"github.com/grafana/globalconf"
	"github.com/grafana/metrictank/util"
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
	MetaRecordTable          string
	Hosts                    string
	CaPath                   string
	Username                 string
	Password                 string
	Consistency              string
	Timeout                  time.Duration
	NumConns                 int
	ProtoVer                 int
	DisableInitialHostLookup bool
	InitLoadConcurrency      int

	// tableSchemas get set by reading and parsing the SchemaFile in the method ParseSchemas()
	schemaKeyspace        string
	schemaTable           string
	schemaArchiveTable    string
	schemaMetaRecordTable string
}

// NewIdxConfig returns IdxConfig with default values set.
func NewIdxConfig() *IdxConfig {
	return &IdxConfig{
		Enabled:                  true,
		Hosts:                    "localhost:9042",
		Keyspace:                 "metrictank",
		Table:                    "metric_idx",
		ArchiveTable:             "metric_idx_archive",
		MetaRecordTable:          "meta_records",
		Consistency:              "one",
		Timeout:                  time.Second,
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

func (cfg *IdxConfig) ParseSchemas(schemaFileReader io.Reader) error {
	tableSchemas, err := util.ReadAllEntries(schemaFileReader)
	if err != nil {
		return fmt.Errorf("Failed to read schemas from file %s: %s", CliConfig.SchemaFile, err)
	}

	var ok bool
	cfg.schemaKeyspace, ok = tableSchemas["schema_keyspace"]
	if !ok {
		return fmt.Errorf("Table schema section \"schema_keyspace\" is missing")
	}

	cfg.schemaTable, ok = tableSchemas["schema_table"]
	if !ok {
		return fmt.Errorf("Table schema section \"schema_table\" is missing")
	}

	cfg.schemaArchiveTable, ok = tableSchemas["schema_archive_table"]
	if !ok {
		return fmt.Errorf("Table schema section \"schema_archive_table\" is missing")
	}

	if memory.MetaTagSupport {
		cfg.schemaMetaRecordTable, ok = tableSchemas["schema_meta_record_table"]
		if !ok {
			return fmt.Errorf("Table schema section \"schema_meta_record_table\" is missing")
		}
	}

	return nil
}

func (cfg *IdxConfig) ParseSchemasFromSchemaFile() error {
	schemaFileReader, err := os.Open(cfg.SchemaFile)
	if err != nil {
		return fmt.Errorf("Failed to open schema file %s: %s", cfg.SchemaFile, err)
	}
	defer schemaFileReader.Close()

	if err := cfg.ParseSchemas(schemaFileReader); err != nil {
		return fmt.Errorf("cassandra-store: Failed when reading and parsing schemas file. %s", err)
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
	casIdx.StringVar(&CliConfig.MetaRecordTable, "meta-record-table", CliConfig.MetaRecordTable, "Cassandra table to store meta records.")
	casIdx.StringVar(&CliConfig.Consistency, "consistency", CliConfig.Consistency, "write consistency (any|one|two|three|quorum|all|local_quorum|each_quorum|local_one")
	casIdx.DurationVar(&CliConfig.Timeout, "timeout", CliConfig.Timeout, "cassandra request timeout")
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

// ConfigProcess validates CliConfig and parses the table schema file.
// If an error is discovered this will exit with status set to 1.
func ConfigProcess() {
	if err := CliConfig.Validate(); err != nil {
		log.Fatalf("cassandra-idx: Config validation error. %s", err)
	}

	err := CliConfig.ParseSchemasFromSchemaFile()
	if err != nil {
		log.Fatalf("cassandra-idx: Failed when reading and parsing schemas file. %s", err)
	}
}
