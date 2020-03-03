package bigtable

import (
	"flag"
	"time"

	"github.com/grafana/globalconf"
	log "github.com/sirupsen/logrus"
)

type Config struct {
	Enabled           bool
	gcpProject        string
	bigtableInstance  string
	tableName         string
	batchTableName    string
	metaRecordCf      string
	metaRecordBatchCf string
	pollInterval      time.Duration
	pruneInterval     time.Duration
	pruneAge          time.Duration
	updateRecords     bool
	createCf          bool
}

func (cfg *Config) Validate() error {
	return nil
}

// return Config with default values set.
func NewConfig() *Config {
	return &Config{
		Enabled:           false,
		gcpProject:        "default",
		bigtableInstance:  "default",
		tableName:         "meta_records",
		batchTableName:    "meta_record_batches",
		metaRecordCf:      "mr",  // currently not configurable
		metaRecordBatchCf: "mrb", // currently not configurable
		pollInterval:      time.Second * 10,
		pruneInterval:     time.Hour * 24,
		pruneAge:          time.Hour * 72,
		updateRecords:     true,
		createCf:          true,
	}
}

var CliConfig = NewConfig()

func ConfigSetup() {
	btIdx := flag.NewFlagSet("bigtable-meta-record-idx", flag.ExitOnError)

	btIdx.BoolVar(&CliConfig.Enabled, "enabled", CliConfig.Enabled, "")
	btIdx.StringVar(&CliConfig.gcpProject, "gcp-project", CliConfig.gcpProject, "Name of GCP project the bigtable cluster resides in")
	btIdx.StringVar(&CliConfig.bigtableInstance, "bigtable-instance", CliConfig.bigtableInstance, "Name of bigtable instance")
	btIdx.StringVar(&CliConfig.tableName, "table-name", CliConfig.tableName, "Table to store meta records")
	btIdx.StringVar(&CliConfig.batchTableName, "batch-table-name", CliConfig.batchTableName, "Table to store meta data of meta record batches")
	btIdx.DurationVar(&CliConfig.pollInterval, "poll-interval", CliConfig.pollInterval, "Interval at which to poll store for meta record updates")
	btIdx.DurationVar(&CliConfig.pruneInterval, "prune-interval", CliConfig.pruneInterval, "Interval at which meta records of old batches get pruned")
	btIdx.DurationVar(&CliConfig.pruneAge, "prune-age", CliConfig.pruneAge, "The minimum age a batch of meta records must have to be pruned")
	btIdx.BoolVar(&CliConfig.updateRecords, "update-records", CliConfig.updateRecords, "Synchronize meta record changes to bigtable. not all your nodes need to do this")
	btIdx.BoolVar(&CliConfig.createCf, "create-cf", CliConfig.createCf, "Enable the creation of the table and column families")

	globalconf.Register("bigtable-meta-record-idx", btIdx, flag.ExitOnError)
}

func ConfigProcess() {
	if err := CliConfig.Validate(); err != nil {
		log.Fatalf("bt-meta-record-idx: Config validation error. %s", err)
	}
}
