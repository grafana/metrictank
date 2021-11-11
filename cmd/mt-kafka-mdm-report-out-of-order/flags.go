package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/grafana/globalconf"
	"github.com/grafana/metrictank/idx/cassandra"
	inKafkaMdm "github.com/grafana/metrictank/input/kafkamdm"
	log "github.com/sirupsen/logrus"
)

type Flags struct {
	flagSet *flag.FlagSet

	RunDuration   time.Duration
	Config        string
	PartitionFrom int
	PartitionTo   int
	ReorderWindow uint
	Prefix        string
	Substr        string
	GroupByName   bool
	GroupByTag    string
}

func NewFlags() *Flags {
	var flags Flags

	flags.flagSet = flag.NewFlagSet("application flags", flag.ExitOnError)
	flags.flagSet.DurationVar(&flags.RunDuration, "run-duration", 5*time.Minute, "the duration of time to run the program")
	flags.flagSet.StringVar(&flags.Config, "config", "/etc/metrictank/metrictank.ini", "configuration file path")
	flags.flagSet.IntVar(&flags.PartitionFrom, "partition-from", 0, "the partition to load the index from")
	flags.flagSet.IntVar(&flags.PartitionTo, "partition-to", -1, "load the index from all partitions up to this one (exclusive). If unset, only the partition defined with \"--partition-from\" is loaded from")
	flags.flagSet.UintVar(&flags.ReorderWindow, "reorder-window", 1, "the size of the reorder buffer window")
	flags.flagSet.StringVar(&flags.Prefix, "prefix", "", "only report metrics with a name that has this prefix")
	flags.flagSet.StringVar(&flags.Substr, "substr", "", "only report metrics with a name that has this substring")
	flags.flagSet.BoolVar(&flags.GroupByName, "group-by-name", false, "group out-of-order metrics by name")
	flags.flagSet.StringVar(&flags.GroupByTag, "group-by-tag", "", "group out-of-order metrics by the specified tag")

	flags.flagSet.Usage = flags.Usage
	return &flags
}

func (flags *Flags) Parse(args []string) {
	err := flags.flagSet.Parse(args)
	if err != nil {
		log.Fatalf("failed to parse application flags %v: %s", args, err.Error)
		os.Exit(1)
	}

	path := ""
	if _, err := os.Stat(flags.Config); err == nil {
		path = flags.Config
	}
	config, err := globalconf.NewWithOptions(&globalconf.Options{
		Filename:  path,
		EnvPrefix: "MT_",
	})
	if err != nil {
		log.Fatalf("error with configuration file: %s", err.Error())
		os.Exit(1)
	}
	_ = cassandra.ConfigSetup()
	inKafkaMdm.ConfigSetup()
	config.Parse()

	if flags.GroupByName == false && flags.GroupByTag == "" {
		log.Fatalf("must specify at least one of -group-by-name or -group-by-tag")
		os.Exit(1)
	}

	if flags.ReorderWindow < 1 {
		log.Fatalf("-reorder-window must be greater than zero")
		os.Exit(1)
	}
}

func (flags *Flags) Usage() {
	fmt.Fprintln(os.Stderr, "mt-kafka-mdm-report-out-of-order")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Inspects what's flowing through kafka (in mdm format) and reports out of order data grouped by metric name or tag, taking into account the reorder buffer)")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "# Mechanism")
	fmt.Fprintln(os.Stderr, "* it sniffs points being added on a per-series (metric Id) level")
	fmt.Fprintln(os.Stderr, "* for every series, tracks the last 'correct' point.  E.g. a point that was able to be added to the series because its timestamp is higher than any previous timestamp")
	fmt.Fprintln(os.Stderr, "* if for any series, a point comes in with a timestamp equal or lower than the last point correct point - which metrictank would not add unless it falls within the reorder buffer - it triggers an event for this out-of-order point")
	fmt.Fprintln(os.Stderr, "* the reorder buffer is described by the window size")
	fmt.Fprintln(os.Stderr, "Usage:")
	fmt.Fprintln(os.Stderr, "  mt-kafka-mdm-report-out-of-order [flags]")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Example output:")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "  total metric points count=2710806")
	fmt.Fprintln(os.Stderr, "  total out-of-order metric points count=3878")
	fmt.Fprintln(os.Stderr, "  out-of-order metric points grouped by name:")
	fmt.Fprintln(os.Stderr, "  out-of-order metric points for name=\"fruit.weight\" count=4 percentGroup=4.301075 percentClass=0.096131 percentTotal=0.000129")
	fmt.Fprintln(os.Stderr, "  out-of-order metric points for name=\"fruit.height\" count=1 percentGroup=4.545455 percentClass=0.024033 percentTotal=0.000032")
	fmt.Fprintln(os.Stderr, "  ...")
	fmt.Fprintln(os.Stderr, "  out-of-order metric points grouped by tag=\"fruit\":")
	fmt.Fprintln(os.Stderr, "  out-of-order metric points for tag=\"fruit\" value=\"apple\" count=80 percentGroup=5.856515 percentClass=2.062919 percentTotal=0.002951")
	fmt.Fprintln(os.Stderr, "  out-of-order metric points for tag=\"fruit\" value=\"orange\" count=2912 percentGroup=0.306267 percentClass=75.090253 percentTotal=0.107422")
	fmt.Fprintln(os.Stderr, "  ...")
	fmt.Fprintln(os.Stderr, "  total duplicate metric points count=12760")
	fmt.Fprintln(os.Stderr, "  duplicate metric points grouped by name:")
	fmt.Fprintln(os.Stderr, "  duplicate metric points for name=\"fruit.width\" count=105 percentGroup=19.266055 percentClass=0.760704 percentTotal=0.003397")
	fmt.Fprintln(os.Stderr, "  duplicate metric points for name=\"fruit.length\" count=123 percentGroup=15.688776 percentClass=0.891111 percentTotal=0.003979")
	fmt.Fprintln(os.Stderr, "  ...")
	fmt.Fprintln(os.Stderr, "  duplicate metric points grouped by tag=\"fruit\":")
	fmt.Fprintln(os.Stderr, "  duplicate metric points for tag=\"fruit\" value=\"banana\" count=4002 percentGroup=17.201066 percentClass=31.363636 percentTotal=0.147631")
	fmt.Fprintln(os.Stderr, "  duplicate metric points for tag=\"fruit\" value=\"orange\" count=4796 percentGroup=0.504415 percentClass=37.586207 percentTotal=0.176922")
	fmt.Fprintln(os.Stderr, "  ...")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "Fields:")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "  name:         the name of the metric (when grouped by name)")
	fmt.Fprintln(os.Stderr, "  tag:          the tag key (when grouped by tag)")
	fmt.Fprintln(os.Stderr, "  value:        the tag value (when grouped by tag)")
	fmt.Fprintln(os.Stderr, "  count:        the number of metric points")
	fmt.Fprintln(os.Stderr, "                the example above shows that 4002 metric points that had tag \"fruit\"=\"banana\" were duplicates")
	fmt.Fprintln(os.Stderr, "  percentGroup: the percentage of all of the metric points which had the same name/tag (depending on grouping) that were out of order/duplicates (depending on classification)")
	fmt.Fprintln(os.Stderr, "                the example above shows that ~4.301% of all metric points with name \"fruit.weight\" were out of order")
	fmt.Fprintln(os.Stderr, "  percentClass: the percentage of all of the metric points which were out of order/duplicates (depending on classification) that had this name/tag (depending on grouping)")
	fmt.Fprintln(os.Stderr, "                the example above shows that ~2.063% of all metric points that were out of order had tag fruit=apple")
	fmt.Fprintln(os.Stderr, "  percentTotal: the percentage of all metric points that had this name/tag (depending on grouping) and were out of order/duplicates (depending on classification)")
	fmt.Fprintln(os.Stderr, "                the example above shows that ~0.177% of all metric points had tag \"fruit\"=\"orange\" and were duplicates")
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "flags:")
	flags.flagSet.PrintDefaults()
	fmt.Fprintln(os.Stderr)
	fmt.Fprintln(os.Stderr, "EXAMPLES:")
	fmt.Fprintln(os.Stderr, "  mt-kafka-mdm-report-out-of-order -group-by-name -config metrictank.ini -partition-from 0")
	fmt.Fprintln(os.Stderr, "  mt-kafka-mdm-report-out-of-order -group-by-name -group-by-tag namespace -config metrictank.ini -partition-from 0 -partition-to 3 -reorder-window 5 -run-duration 5m")
}

func ParseFlags() Flags {
	flags := NewFlags()

	flag.Usage = flags.Usage

	flags.Parse(os.Args[1:])

	return *flags
}
