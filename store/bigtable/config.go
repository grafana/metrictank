package bigtable

import (
	"flag"
	"time"

	"github.com/rakyll/globalconf"
)

type StoreConfig struct {
	Enabled           bool
	GcpProject        string
	BigtableInstance  string
	TableName         string
	WriteQueueSize    int
	WriteMaxFlushSize int
	WriteConcurrency  int
	ReadConcurrency   int
	MaxChunkSpan      time.Duration
	ReadTimeout       time.Duration
	WriteTimeout      time.Duration
	CreateCF          bool
}

// return StoreConfig with default values set.
func NewStoreConfig() *StoreConfig {
	return &StoreConfig{
		Enabled:           false,
		GcpProject:        "default",
		BigtableInstance:  "default",
		TableName:         "metrics",
		WriteQueueSize:    100000,
		WriteMaxFlushSize: 10000,
		WriteConcurrency:  10,
		ReadConcurrency:   20,
		MaxChunkSpan:      time.Hour * 6,
		ReadTimeout:       time.Second * 5,
		WriteTimeout:      time.Second * 5,
		CreateCF:          true,
	}
}

var CliConfig = NewStoreConfig()

func ConfigSetup() {
	btStore := flag.NewFlagSet("bigtable-store", flag.ExitOnError)
	btStore.BoolVar(&CliConfig.Enabled, "enabled", CliConfig.Enabled, "enable the bigtable backend store plugin")
	btStore.StringVar(&CliConfig.GcpProject, "gcp-project", CliConfig.GcpProject, "Name of GCP project the bigtable cluster resides in")
	btStore.StringVar(&CliConfig.BigtableInstance, "bigtable-instance", CliConfig.BigtableInstance, "Name of bigtable instance")
	btStore.StringVar(&CliConfig.TableName, "table-name", CliConfig.TableName, "Name of bigtable table used for chunks")
	btStore.IntVar(&CliConfig.WriteQueueSize, "write-queue-size", CliConfig.WriteQueueSize, "Max number of chunks, per write thread, allowed to be unwritten to bigtable. Must be larger then write-max-flush-size")
	btStore.IntVar(&CliConfig.WriteMaxFlushSize, "write-max-flush-size", CliConfig.WriteMaxFlushSize, "Max number of chunks in each batch write to bigtable")
	btStore.IntVar(&CliConfig.WriteConcurrency, "write-concurrency", CliConfig.WriteConcurrency, "Number of writer threads to use")
	btStore.IntVar(&CliConfig.ReadConcurrency, "read-concurrency", CliConfig.ReadConcurrency, "Number concurrent reads that can be processed")
	btStore.DurationVar(&CliConfig.MaxChunkSpan, "max-chunkspan", CliConfig.MaxChunkSpan, "Maximum chunkspan size used.")
	btStore.DurationVar(&CliConfig.ReadTimeout, "read-timeout", CliConfig.ReadTimeout, "read timeout")
	btStore.DurationVar(&CliConfig.WriteTimeout, "write-timeout", CliConfig.WriteTimeout, "write timeout")
	btStore.BoolVar(&CliConfig.CreateCF, "create-cf", CliConfig.CreateCF, "enable the creation of the table and column families")

	globalconf.Register("bigtable-store", btStore)
	return
}

func ConfigProcess() {
	return
}
