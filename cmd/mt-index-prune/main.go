package main

import (
	"flag"
	"fmt"
	"os"
	"regexp"
	"time"

	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/idx/cassandra"
	"github.com/grafana/metrictank/idx/memory"
	"github.com/grafana/metrictank/logger"
	"github.com/grafana/metrictank/mdata"
	"github.com/raintank/schema"
	log "github.com/sirupsen/logrus"
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

type counters struct {
	total         int
	active        int
	defsToArchive int
	defsToDelete  int
	archived      int
	deleted       int
}

func (c *counters) PrintCounters() {
	fmt.Println(fmt.Sprintf("Total analyzed defs: %d", c.total))
	fmt.Println(fmt.Sprintf("Active defs:         %d", c.active))
	fmt.Println(fmt.Sprintf("Defs to Archive:     %d", c.defsToArchive))
	fmt.Println(fmt.Sprintf("Defs to Delete defs:       %d", c.defsToDelete))
	fmt.Println(fmt.Sprintf("Defs Archived:       %d", c.archived))
	fmt.Println(fmt.Sprintf("Defs Deleted:       %d", c.deleted))
}

func main() {
	var noDryRun, verbose, tagSupport bool
	var partitionFrom, partitionTo int
	var indexRulesFile, schemasFile string
	globalFlags := flag.NewFlagSet("global config flags", flag.ExitOnError)
	globalFlags.BoolVar(&noDryRun, "no-dry-run", false, "do not only plan and print what to do, but also execute it")
	globalFlags.BoolVar(&verbose, "verbose", false, "print every metric name that gets archived")
	globalFlags.BoolVar(&tagSupport, "tag-support", false, "defines if tags should be taken into account. This is relevant to match metric names against the schema definitions")
	globalFlags.IntVar(&partitionFrom, "partition-from", 0, "the partition to start at")
	globalFlags.IntVar(&partitionTo, "partition-to", -1, "prune all partitions up to this one (exclusive). If unset, only the partition defined with \"--partition-from\" gets pruned")
	globalFlags.StringVar(&indexRulesFile, "index-rules-file", "/etc/metrictank/index-rules.conf", "name of file which defines the max-stale times")
	globalFlags.StringVar(&schemasFile, "schemas-file", "/etc/metrictank/storage-schemas.conf", "name of file which defines the storage schemas")
	cassFlags := cassandra.ConfigSetup()

	flag.Usage = func() {
		fmt.Println("mt-index-prune")
		fmt.Println()
		fmt.Println("Retrieves a metrictank index and moves all deprecated entries into an archive table")
		fmt.Println()
		fmt.Printf("Usage:\n\n")
		fmt.Printf("  mt-index-prune [global config flags] <idxtype> [idx config flags]\n\n")
		fmt.Printf("global config flags:\n\n")
		globalFlags.PrintDefaults()
		fmt.Println()
		fmt.Printf("idxtype: only 'cass' supported for now\n\n")
		fmt.Printf("cass config flags:\n\n")
		cassFlags.PrintDefaults()
		fmt.Println()
		fmt.Println()
		fmt.Println("EXAMPLES:")
		fmt.Println("mt-index-prune --verbose --partition-from 0 --partition-to 8 cass -hosts cassandra:9042")
	}

	if len(os.Args) == 2 && (os.Args[1] == "-h" || os.Args[1] == "--help") {
		flag.Usage()
		os.Exit(0)
	}

	if len(os.Args) < 2 {
		flag.Usage()
		os.Exit(-1)
	}

	var cassI int
	for i, v := range os.Args {
		if v == "cass" {
			cassI = i
		}
	}

	if cassI == 0 {
		log.Println("only indextype 'cass' supported")
		flag.Usage()
		os.Exit(1)
	}
	globalFlags.Parse(os.Args[1:cassI])

	indexRules, err := conf.ReadIndexRules(indexRulesFile)
	if os.IsNotExist(err) {
		log.Fatalf("Index-rules.conf file %s does not exist; exiting", indexRulesFile)
		os.Exit(1)
	}
	now := time.Now()
	cutoffs := indexRules.Cutoffs(now)

	mdata.Schemas, err = conf.ReadSchemas(schemasFile)
	if err != nil {
		log.Fatalf("Error in reading storage schemas file: %s", err)
		os.Exit(1)
	}

	cassFlags.Parse(os.Args[cassI+1:])
	cassandra.CliConfig.Enabled = true
	cassIdx := cassandra.New(cassandra.CliConfig)
	err = cassIdx.InitBare()
	perror(err)
	err = cassIdx.EnsureArchiveTableExists(nil)
	perror(err)

	// we don't want to filter any metric definitions during the loading
	// so MaxStale is set to 0
	memory.IndexRules = conf.IndexRules{
		Rules: nil,
		Default: conf.IndexRule{
			Name:     "default",
			Pattern:  regexp.MustCompile(""),
			MaxStale: 0,
		},
	}

	defCounters := counters{}
	defs := make([]schema.MetricDefinition, 0)
	defsToArchive := make([]schema.MetricDefinition, 0)
	defsToDelete := make([]schema.MetricDefinition, 0)

	for partition := partitionFrom; (partitionTo == -1 && partition == partitionFrom) || (partitionTo > 0 && partition < partitionTo); partition++ {
		log.Infof("starting to process partition %d", partition)
		defsByNameWithTags := make(map[string][]schema.MetricDefinition)
		defs = cassIdx.LoadPartitions([]int32{int32(partition)}, defs, now)
		defCounters.total += len(defs)

		for _, def := range defs {
			var schema conf.Schema
			if tagSupport {
				_, schema = mdata.MatchSchema(def.NameWithTags(), def.Interval)
			} else {
				_, schema = mdata.MatchSchema(def.Name, def.Interval)
			}
			// we only care about the longest retention period, that is always the last one
			longestRetentionPeriod := schema.Retentions[len(schema.Retentions)-1].MaxRetention()
			if def.LastUpdate+int64(longestRetentionPeriod) < now.Unix() {
				defsToDelete = append(defsToDelete, def)
				defCounters.defsToDelete++
				continue
			}

			name := def.NameWithTags()
			defsByNameWithTags[name] = append(defsByNameWithTags[name], def)
		}

		for name, defs := range defsByNameWithTags {
			// find the latest LastUpdate ts
			latest := int64(0)
			for _, def := range defs {
				if def.LastUpdate > latest {
					latest = def.LastUpdate
				}
			}

			irId, _ := indexRules.Match(name)
			if latest < cutoffs[irId] {
				for _, def := range defs {
					defsToArchive = append(defsToArchive, def)
				}
				defCounters.defsToArchive += len(defs)

				if verbose {
					fmt.Println(fmt.Sprintf("Metric is deprecated: %s", name))
				}
			} else {
				defCounters.active += len(defs)

				if verbose {
					fmt.Println(fmt.Sprintf("Metric is active: %s", name))
				}
			}
		}

		if noDryRun {
			count, err := cassIdx.ArchiveDefs(defsToArchive)
			log.Infof("archiving request complete. successful=%d", count)
			if count != len(defsToArchive) {
				log.Warnf("some defs failed to be archived. failed=%d", len(defsToArchive)-count)
			}
			if err != nil {
				log.Warnf("Failed to archive defs: %s", err.Error())
			}
			defCounters.archived += count

			count, err = cassIdx.DeleteDefs(defsToDelete)
			log.Infof("delete request complete. successful=%d", count)
			if count != len(defsToDelete) {
				log.Warnf("some defs failed to be deleted. failed=%d", len(defsToDelete)-count)
			}
			if err != nil {
				log.Warnf("Failed to delete defs: %s", err.Error())
			}
			defCounters.deleted += count
		}

		defs = defs[:0]
		defsToArchive = defsToArchive[:0]
		defsToDelete = defsToDelete[:0]
	}

	defCounters.PrintCounters()
}
