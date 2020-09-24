package main

import (
	"flag"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/metrictank/cmd/mt-index-cat/out"
	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/idx/bigtable"
	"github.com/grafana/metrictank/idx/cassandra"
	"github.com/grafana/metrictank/idx/memory"
	"github.com/grafana/metrictank/logger"
	"github.com/grafana/metrictank/schema"
	"github.com/raintank/dur"
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

func main() {

	var addr string
	var prefix string
	var substr string
	var suffix string
	var regexStr string
	var regex *regexp.Regexp
	var tags string
	var from string
	var maxStale string
	var minStale string
	var verbose bool
	var limit int
	var partitionStr string
	var btTotalPartitions int

	globalFlags := flag.NewFlagSet("global config flags", flag.ExitOnError)
	globalFlags.StringVar(&addr, "addr", "http://localhost:6060", "graphite/metrictank address")
	globalFlags.StringVar(&prefix, "prefix", "", "only show metrics that have this prefix")
	globalFlags.StringVar(&substr, "substr", "", "only show metrics that have this substring")
	globalFlags.StringVar(&suffix, "suffix", "", "only show metrics that have this suffix")
	globalFlags.StringVar(&partitionStr, "partitions", "*", "only show metrics from the comma separated list of partitions or * for all")
	globalFlags.IntVar(&btTotalPartitions, "bt-total-partitions", -1, "when using bigtable you must set this to the total number of partitions for the instance if you do not specify partitions with the 'partitions' setting")
	globalFlags.StringVar(&regexStr, "regex", "", "only show metrics that match this regex")
	globalFlags.StringVar(&tags, "tags", "", "tag filter. empty (default), 'some', 'none', 'valid', or 'invalid'")
	globalFlags.StringVar(&from, "from", "30min", "for vegeta outputs, will generate requests for data starting from now minus... eg '30min', '5h', '14d', etc. or a unix timestamp")
	globalFlags.StringVar(&maxStale, "max-stale", "6h30min", "exclude series that have not been seen for this much time (compared against LastUpdate).  use 0 to disable")
	globalFlags.StringVar(&minStale, "min-stale", "0", "exclude series that have been seen in this much time (compared against LastUpdate).  use 0 to disable")
	globalFlags.IntVar(&limit, "limit", 0, "only show this many metrics.  use 0 to disable")
	globalFlags.BoolVar(&verbose, "verbose", false, "print stats to stderr")

	cassFlags := cassandra.ConfigSetup()
	btFlags := bigtable.ConfigSetup()

	outputs := []string{"dump", "list", "vegeta-render", "vegeta-render-patterns"}

	flag.Usage = func() {
		fmt.Println("mt-index-cat")
		fmt.Println()
		fmt.Println("Retrieves a metrictank index and dumps it in the requested format")
		fmt.Println("In particular, the vegeta outputs are handy to pipe requests for given series into the vegeta http benchmark tool")
		fmt.Println()
		fmt.Printf("Usage:\n\n")
		fmt.Printf("  mt-index-cat [global config flags] <idxtype> [idx config flags] output \n\n")
		fmt.Printf("global config flags:\n\n")
		globalFlags.PrintDefaults()
		fmt.Println()
		fmt.Println("tags filter:")
		fmt.Println("     ''        no filtering based on tags")
		fmt.Println("     'none'    only show metrics that have no tags")
		fmt.Println("     'some'    only show metrics that have one or more tags")
		fmt.Println("     'valid'   only show metrics whose tags (if any) are valid")
		fmt.Println("     'invalid' only show metrics that have one or more invalid tags")
		fmt.Println()
		fmt.Printf("\n\n")
		fmt.Printf("cass config flags:\n\n")
		cassFlags.PrintDefaults()
		fmt.Printf("\n\n")
		fmt.Printf("bigtable config flags:\n\n")
		btFlags.PrintDefaults()
		fmt.Printf("\n\n")
		fmt.Println()
		fmt.Println("output:")
		fmt.Println()
		fmt.Printf(" * presets: %v\n", strings.Join(outputs, "|"))
		fmt.Println(" * templates, which may contain:")
		fmt.Println("   - fields,  e.g. '{{.Id}} {{.OrgId}} {{.Name}} {{.Interval}} {{.Unit}} {{.Mtype}} {{.Tags}} {{.LastUpdate}} {{.Partition}}'")
		fmt.Println("   - methods, e.g. '{{.NameWithTags}}' (works basically the same as a field)")
		fmt.Println("   - processing functions:")
		fmt.Println("     pattern:       transforms a graphite.style.metric.name into a pattern with wildcards inserted")
		fmt.Println("                    an operation is randomly selected between: replacing a node with a wildcard, replacing a character with a wildcard, and passthrough")
		out.PatternCustomUsage("     ")
		fmt.Println("     age:           subtracts the passed integer (typically .LastUpdate) from the query time")
		fmt.Println("     roundDuration: formats an integer-seconds duration using aggressive rounding. for the purpose of getting an idea of overal metrics age")
		fmt.Println()
		fmt.Println()
		fmt.Println("Cassandra Examples:")
		fmt.Println("mt-index-cat -from 60min cass -hosts cassandra:9042 list")
		fmt.Println("mt-index-cat -from 60min cass -hosts cassandra:9042 'sumSeries({{.Name | pattern}})'")
		fmt.Println("mt-index-cat -from 60min cass -hosts cassandra:9042 'GET http://localhost:6060/render?target=sumSeries({{.Name | pattern}})&from=-6h\\nX-Org-Id: 1\\n\\n'")
		fmt.Println("mt-index-cat cass -hosts cassandra:9042 -timeout 60s '{{.LastUpdate | age | roundDuration}}\\n' | sort | uniq -c")
		fmt.Println("mt-index-cat cass -hosts localhost:9042 -schema-file ../../scripts/config/schema-idx-cassandra.toml '{{.Name | patternCustom 15 \"pass\" 40 \"1rcnw\" 15 \"2rcnw\" 10 \"3rcnw\" 10 \"3rccw\" 10 \"2rccw\"}}\\n'")
		fmt.Println()
		fmt.Println()
		fmt.Println("Bigtable Examples:")
		fmt.Println("mt-index-cat -max-stale 0 -bt-total-partitions 128 bt -gcp-project your_project -bigtable-instance the_bt_instance -table-name metric_idx -create-cf false list")
		fmt.Println("mt-index-cat -max-stale 768h -partitions 1,2,3 bt -gcp-project your_project -bigtable-instance the_bt_instance -table-name metric_idx -create-cf false '{{.NameWithTags}} {{.Id}} {{.OrgId}} {{.LastUpdate}} {{.Partition}}\n'")

	}

	if len(os.Args) == 2 && (os.Args[1] == "-h" || os.Args[1] == "--help") {
		flag.Usage()
		os.Exit(0)
	}

	if len(os.Args) < 3 {
		flag.Usage()
		os.Exit(-1)
	}

	format := os.Args[len(os.Args)-1]
	var found bool
	if strings.Contains(format, "{{") {
		found = true
	} else {
		for _, output := range outputs {
			if format == output {
				found = true
				break
			}
		}
	}

	if !found {
		log.Printf("invalid output %q", format)
		flag.Usage()
		os.Exit(-1)
	}
	var cassI int
	var btI int
	for i, v := range os.Args {
		if v == "cass" {
			cassI = i
		}
		if v == "bt" {
			btI = i
		}
	}

	if cassI > 0 && btI > 0 {
		log.Println("you may only use one index type at a time, either 'cass' or 'bt'")
		flag.Usage()
		os.Exit(1)
	}

	if cassI == 0 && btI == 0 {
		log.Println("you must use one index type, either 'cass' or 'bt'")
		flag.Usage()
		os.Exit(1)
	}

	if tags != "" && tags != "valid" && tags != "invalid" && tags != "some" && tags != "none" {
		log.Println("invalid tags filter")
		flag.Usage()
		os.Exit(1)
	}

	var idxFlagPos int
	if cassI > 0 {
		idxFlagPos = cassI
	} else {
		idxFlagPos = btI
	}

	globalFlags.Parse(os.Args[1:idxFlagPos])
	if cassI > 0 {
		cassFlags.Parse(os.Args[idxFlagPos+1 : len(os.Args)-1])
		cassandra.CliConfig.Enabled = true
	} else {
		btFlags.Parse(os.Args[idxFlagPos+1 : len(os.Args)-1])
		bigtable.CliConfig.Enabled = true
	}

	if regexStr != "" {
		var err error
		regex, err = regexp.Compile(regexStr)
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(2)
		}
	}

	var show func(d schema.MetricDefinition)

	switch format {
	case "dump":
		show = out.Dump
	case "list":
		show = out.List
	case "vegeta-render":
		show = out.GetVegetaRender(addr, from)
	case "vegeta-render-patterns":
		show = out.GetVegetaRenderPattern(addr, from)
	default:
		show = out.Template(format)
	}

	var cassIdx *cassandra.CasIdx
	var btIdx *bigtable.BigtableIdx
	var err error

	if cassI > 0 {
		cassIdx = cassandra.New(cassandra.CliConfig)
		err = cassIdx.InitBare()
	} else {
		btIdx = bigtable.New(bigtable.CliConfig)
		err = btIdx.InitBare()
	}
	perror(err)

	// from should either be a unix timestamp, or a specification that graphite/metrictank will recognize.
	_, err = strconv.Atoi(from)
	if err != nil {
		_, err = dur.ParseNDuration(from)
		perror(err)
	}

	memory.IndexRules = conf.IndexRules{
		Rules: nil,
		Default: conf.IndexRule{
			Name:     "default",
			Pattern:  regexp.MustCompile(""),
			MaxStale: 0,
		},
	}

	if maxStale != "0" {
		maxStaleInt, err := dur.ParseNDuration(maxStale)
		perror(err)
		memory.IndexRules.Default.MaxStale = time.Duration(maxStaleInt) * time.Second
	}

	var cutoffMin int64
	if minStale != "0" {
		minStaleInt, err := dur.ParseNDuration(minStale)
		perror(err)
		cutoffMin = time.Now().Unix() - int64(minStaleInt)
	}

	var partitions []int32
	if partitionStr != "*" {
		for _, p := range strings.Split(partitionStr, ",") {
			p = strings.TrimSpace(p)

			// handle trailing "," on the list of partitions.
			if p == "" {
				continue
			}

			id, err := strconv.ParseInt(p, 10, 32)
			if err != nil {
				log.Printf("invalid partition id %q. must be a int32", p)
				flag.Usage()
				os.Exit(-1)
			}
			partitions = append(partitions, int32(id))
		}
	}

	// if partitionStr is set to all (*) and we are using bigtable then we must
	// ensure that we know the total number of partitions
	if partitionStr == "*" && btI > 0 {
		if btTotalPartitions == -1 {
			log.Println("When selecting all partitions with bigtable you must specify the total number of partitions for the instance")
			flag.Usage()
			os.Exit(-1)
		} else {
			for i := 0; i < btTotalPartitions; i++ {
				partitions = append(partitions, int32(i))
			}
		}
	}

	var total int
	var shown int

	processDefs := func(defs []schema.MetricDefinition) {
		total += len(defs)
		if shown >= limit && limit > 0 {
			log.Infof("Limit (%d) reached while processing Metric Definitions", limit)
			return
		}
		for _, d := range defs {
			// note that prefix and substr can be "", meaning filter disabled.
			// the conditions handle this fine as well.
			if !strings.HasPrefix(d.Name, prefix) {
				continue
			}
			if !strings.HasSuffix(d.Name, suffix) {
				continue
			}
			if !strings.Contains(d.Name, substr) {
				continue
			}
			if tags == "none" && len(d.Tags) != 0 {
				continue
			}
			if tags == "some" && len(d.Tags) == 0 {
				continue
			}
			if regex != nil && !regex.MatchString(d.Name) {
				continue
			}
			if tags == "valid" || tags == "invalid" {
				err := schema.ValidateTags(d.Tags)

				// skip the metric if the validation result is not what we want
				if (err == nil) != (tags == "valid") {
					continue
				}
			}
			if cutoffMin != 0 && d.LastUpdate >= cutoffMin {
				continue
			}
			show(d)
			shown++
			if shown >= limit && limit > 0 {
				log.Infof("Limit (%d) reached while processing Metric Definitions", limit)
				return
			}
		}
	}

	var defs []schema.MetricDefinition
	if len(partitions) == 0 {
		if cassI > 0 {
			defs = cassIdx.Load(nil, time.Now())
			// set this after doing the query, to assure age can't possibly be negative unless if clocks are misconfigured.
			out.QueryTime = time.Now().Unix()
			processDefs(defs)
		}
	} else {
		if cassI > 0 {
			defs = cassIdx.LoadPartitions(partitions, nil, time.Now())
			// set this after doing the query, to assure age can't possibly be negative unless if clocks are misconfigured.
			out.QueryTime = time.Now().Unix()
			processDefs(defs)
		} else {
			now := time.Now()
			for _, p := range partitions {
				defs = btIdx.LoadPartition(p, nil, now)
				// set this after doing the query, to assure age can't possibly be negative unless if clocks are misconfigured.
				out.QueryTime = time.Now().Unix()
				processDefs(defs)
			}
		}
	}

	if verbose {
		fmt.Fprintf(os.Stderr, "total: %d\n", total)
		fmt.Fprintf(os.Stderr, "shown: %d\n", shown)
	}
}
