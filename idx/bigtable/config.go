package bigtable

import (
	"errors"
	"flag"
	"time"

	"github.com/grafana/globalconf"
	log "github.com/sirupsen/logrus"
)

type IdxConfig struct {
	Enabled           bool
	GcpProject        string
	BigtableInstance  string
	TableName         string
	WriteQueueSize    int
	WriteMaxFlushSize int
	WriteConcurrency  int
	UpdateBigtableIdx bool
	UpdateInterval    time.Duration
	updateInterval32  uint32
	PruneInterval     time.Duration
	CreateCF          bool
}

func (cfg *IdxConfig) Validate() error {
	cfg.updateInterval32 = uint32(cfg.UpdateInterval.Nanoseconds() / int64(time.Second))
	if cfg.WriteMaxFlushSize > 100000 {
		return errors.New("write-max-flush-size must be <= 100000.")
	}
	if cfg.WriteMaxFlushSize >= cfg.WriteQueueSize {
		return errors.New("write-queue-size must be larger then write-max-flush-size")
	}
	if cfg.PruneInterval == 0 {
		return errors.New("pruneInterval must be greater then 0")
	}
	return nil
}

// return StoreConfig with default values set.
func NewIdxConfig() *IdxConfig {
	return &IdxConfig{
		Enabled:           false,
		GcpProject:        "default",
		BigtableInstance:  "default",
		TableName:         "metrics",
		WriteQueueSize:    100000,
		WriteMaxFlushSize: 10000,
		WriteConcurrency:  5,
		UpdateBigtableIdx: true,
		UpdateInterval:    time.Hour * 3,
		PruneInterval:     time.Hour * 3,
		CreateCF:          true,
	}
}

var CliConfig = NewIdxConfig()

func ConfigSetup() {
	btIdx := flag.NewFlagSet("bigtable-idx", flag.ExitOnError)

	btIdx.BoolVar(&CliConfig.Enabled, "enabled", CliConfig.Enabled, "")
	btIdx.StringVar(&CliConfig.GcpProject, "gcp-project", CliConfig.GcpProject, "Name of GCP project the bigtable cluster resides in")
	btIdx.StringVar(&CliConfig.BigtableInstance, "bigtable-instance", CliConfig.BigtableInstance, "Name of bigtable instance")
	btIdx.StringVar(&CliConfig.TableName, "table-name", CliConfig.TableName, "Name of bigtable table used for metricDefs")
	btIdx.IntVar(&CliConfig.WriteQueueSize, "write-queue-size", CliConfig.WriteQueueSize, "Max number of metricDefs allowed to be unwritten to bigtable. Must be larger then write-max-flush-size")
	btIdx.IntVar(&CliConfig.WriteMaxFlushSize, "write-max-flush-size", CliConfig.WriteMaxFlushSize, "Max number of metricDefs in each batch write to bigtable")
	btIdx.IntVar(&CliConfig.WriteConcurrency, "write-concurrency", CliConfig.WriteConcurrency, "Number of writer threads to use")
	btIdx.BoolVar(&CliConfig.UpdateBigtableIdx, "update-bigtable-index", CliConfig.UpdateBigtableIdx, "synchronize index changes to bigtable. not all your nodes need to do this.")
	btIdx.DurationVar(&CliConfig.UpdateInterval, "update-interval", CliConfig.UpdateInterval, "frequency at which we should update the metricDef lastUpdate field, use 0s for instant updates")
	btIdx.DurationVar(&CliConfig.PruneInterval, "prune-interval", CliConfig.PruneInterval, "Interval at which the index should be checked for stale series.")
	btIdx.BoolVar(&CliConfig.CreateCF, "create-cf", CliConfig.CreateCF, "enable the creation of the table and column families")

	globalconf.Register("bigtable-idx", btIdx, flag.ExitOnError)
}

func ConfigProcess() {
	if err := CliConfig.Validate(); err != nil {
		log.Fatalf("bigtable-idx: Config validation error. %s", err)
	}
}
