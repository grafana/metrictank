package main

import (
	"flag"
	"fmt"
	"os"
	"regexp"
	"sync"
	"time"

	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/idx/cassandra"
	"github.com/grafana/metrictank/idx/memory"
	"github.com/grafana/metrictank/logger"

	log "github.com/sirupsen/logrus"
)

var (
	numPartitions = 512
	aggregations  conf.Aggregations
	indexRules    conf.IndexRules
	schemas       conf.Schemas

	aggFile        = "/etc/metrictank/storage-aggregation.conf"
	indexRulesFile = "/etc/metrictank/index-rules.conf"
	schemasFile    = "/etc/metrictank/storage-schemas.conf"
)

func init() {
	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)
}

func perror(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}

func main() {
	cassandra.CliConfig.CreateKeyspace = false
	fl := cassandra.ConfigSetup()
	fl.StringVar(&aggFile, "aggregations-file", aggFile, "path to storage-aggregation.conf file")
	fl.StringVar(&indexRulesFile, "rules-file", indexRulesFile, "path to index-rules.conf file")
	fl.StringVar(&schemasFile, "schemas-file", schemasFile, "path to storage-schemas.conf file")
	fl.IntVar(&numPartitions, "num-partitions", numPartitions, "amount of partitions to look for")

	flag.Usage = func() {
		fmt.Println("mt-index-analyzer")
		fmt.Println("")
		fmt.Println("analyzes the contents of your index")
		fmt.Println("limitations:")
		fmt.Println(" - does not take into account series churn. more specifically, assumes each series has data for its entire defined retention")
		fmt.Println(" - assumes chunks are filled corresponding to the interval, doesn't account for sparse data or too-high resolution data")
		fl.PrintDefaults()
	}
	if len(os.Args) == 2 && (os.Args[1] == "-h" || os.Args[1] == "--help") {
		flag.Usage()
		os.Exit(0)
	}

	fl.Parse(os.Args[1:])
	cassandra.CliConfig.Enabled = true
	cassandra.ConfigProcess()
	idx := cassandra.New(cassandra.CliConfig)
	err := idx.InitBare()
	perror(err)

	// for loading the index, to not drop any entries
	memory.IndexRules = conf.IndexRules{
		Rules: nil,
		Default: conf.IndexRule{
			Name:     "default",
			Pattern:  regexp.MustCompile(""),
			MaxStale: 0,
		},
	}

	loadConfFiles()

	irDist = make([]int, len(indexRules.Rules)+1)   // we need one extra to mark the default case
	aggDist = make([]int, len(aggregations.Data)+1) // we need one extra to mark the default case
	schemaDist = make([]int, len(schemas.Index)+1)  // we need one extra to mark the default case
	comboDist = make(map[combo]int)

	jobs := make(chan Job, 10000)

	go loadJobs(idx, jobs)

	var wg sync.WaitGroup
	wg.Add(10)
	for i := 0; i < 10; i++ {
		go processJobs(&wg, jobs)
	}
	wg.Wait()

	printStats()
}

// Job is the minimal representation of a MetricDefinition
// it only serves to compute stats
type Job struct {
	path     string
	interval int
}

func loadConfFiles() {
	var err error

	aggregations, err = conf.ReadAggregations(aggFile)
	if os.IsNotExist(err) {
		log.Infof("Could not read %s: %s: using defaults", aggFile, err)
		aggregations = conf.NewAggregations()
	} else if err != nil {
		log.Fatalf("can't read storage-aggregation file %q: %s", aggFile, err.Error())
	}

	indexRules, err = conf.ReadIndexRules(indexRulesFile)
	if os.IsNotExist(err) {
		log.Infof("Index-rules.conf file %s does not exist; using defaults", indexRulesFile)
		indexRules = conf.NewIndexRules()
	} else if err != nil {
		log.Fatalf("can't read index-rules file %q: %s", indexRulesFile, err.Error())
	}

	schemas, err = conf.ReadSchemas(schemasFile)
	if err != nil {
		log.Fatalf("can't read schemas file %q: %s", schemasFile, err.Error())
	}
}

func loadJobs(idx *cassandra.CasIdx, jobs chan Job) {
	for part := int32(0); part < int32(numPartitions); part++ {
		fmt.Println("loading partition", part)
		// load each partition one by one
		defs := idx.LoadPartitions([]int32{part}, nil, time.Now())
		for _, def := range defs {
			jobs <- Job{
				path:     def.NameWithTags(),
				interval: def.Interval,
			}
		}
	}
	close(jobs)
}

func processJobs(wg *sync.WaitGroup, jobs chan Job) {
	for job := range jobs {
		schemaID, _ := schemas.Match(job.path, job.interval)
		aggID, _ := aggregations.Match(job.path)
		irID, _ := indexRules.Match(job.path)

		schema := schemas.Get(schemaID)
		agg := aggregations.Get(aggID)

		var pointsInTank uint64
		var pointsInStore uint64

		numSeries := uint64(1) // default value, for raw data

		for i, ret := range schema.Retentions {
			pointsInStore += uint64(ret.NumberOfPoints) * numSeries
			pointsInTank += uint64(ret.NumChunks*ret.ChunkSpan) * numSeries / uint64(ret.SecondsPerPoint)

			// if we are about to process rollup archives, we must know how many series we use
			if i == 0 && len(schema.Retentions) > 0 {
				numSeries = uint64(conf.NumSeries(agg.AggregationMethod))
			}
		}
		addStat(aggID, irID, schemaID, pointsInTank, pointsInStore)
	}
	wg.Done()
}
