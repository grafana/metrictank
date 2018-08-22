package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/grafana/metrictank/cmd/mt-index-cat/out"
	"github.com/grafana/metrictank/idx/cassandra"
	"github.com/raintank/dur"
	"github.com/raintank/schema"
)

func perror(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {

	var addr string
	var prefix string
	var substr string
	var suffix string
	var tags string
	var from string
	var maxAge string
	var verbose bool
	var limit int

	globalFlags := flag.NewFlagSet("global config flags", flag.ExitOnError)
	globalFlags.StringVar(&addr, "addr", "http://localhost:6060", "graphite/metrictank address")
	globalFlags.StringVar(&prefix, "prefix", "", "only show metrics that have this prefix")
	globalFlags.StringVar(&substr, "substr", "", "only show metrics that have this substring")
	globalFlags.StringVar(&suffix, "suffix", "", "only show metrics that have this suffix")
	globalFlags.StringVar(&tags, "tags", "", "tag filter. empty (default), 'some', 'none', 'valid', or 'invalid'")
	globalFlags.StringVar(&from, "from", "30min", "for vegeta outputs, will generate requests for data starting from now minus... eg '30min', '5h', '14d', etc. or a unix timestamp")
	globalFlags.StringVar(&maxAge, "max-age", "6h30min", "max age (last update diff with now) of metricdefs.  use 0 to disable")
	globalFlags.IntVar(&limit, "limit", 0, "only show this many metrics.  use 0 to disable")
	globalFlags.BoolVar(&verbose, "verbose", false, "print stats to stderr")

	cassFlags := cassandra.ConfigSetup()

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
		fmt.Printf("idxtype: only 'cass' supported for now\n\n")
		fmt.Printf("cass config flags:\n\n")
		cassFlags.PrintDefaults()
		fmt.Println()
		fmt.Printf("output: either presets like %v\n", strings.Join(outputs, "|"))
		fmt.Printf("output: or custom templates like '{{.Id}} {{.OrgId}} {{.Name}} {{.Metric}} {{.Interval}} {{.Unit}} {{.Mtype}} {{.Tags}} {{.LastUpdate}} {{.Partition}}'\n\n\n")
		fmt.Println("You may also use processing functions in templates:")
		fmt.Println("pattern: transforms a graphite.style.metric.name into a pattern with wildcards inserted")
		fmt.Println("age: subtracts the passed integer (typically .LastUpdate) from the query time")
		fmt.Println("roundDuration: formats an integer-seconds duration using aggressive rounding. for the purpose of getting an idea of overal metrics age")
		fmt.Println("EXAMPLES:")
		fmt.Println("mt-index-cat -from 60min cass -hosts cassandra:9042 list")
		fmt.Println("mt-index-cat -from 60min cass -hosts cassandra:9042 'sumSeries({{.Name | pattern}})'")
		fmt.Println("mt-index-cat -from 60min cass -hosts cassandra:9042 'GET http://localhost:6060/render?target=sumSeries({{.Name | pattern}})&from=-6h\\nX-Org-Id: 1\\n\\n'")
		fmt.Println("mt-index-cat cass -hosts cassandra:9042 -timeout 60s '{{.LastUpdate | age | roundDuration}}\\n' | sort | uniq -c")
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

	if tags != "" && tags != "valid" && tags != "invalid" && tags != "some" && tags != "none" {
		log.Println("invalid tags filter")
		flag.Usage()
		os.Exit(1)
	}

	globalFlags.Parse(os.Args[1:cassI])
	cassFlags.Parse(os.Args[cassI+1 : len(os.Args)-1])
	cassandra.Enabled = true

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

	idx := cassandra.New()
	err := idx.InitBare()
	perror(err)

	// from should either be a unix timestamp, or a specification that graphite/metrictank will recognize.
	_, err = strconv.Atoi(from)
	if err != nil {
		_, err = dur.ParseNDuration(from)
		perror(err)
	}

	var cutoff uint32
	if maxAge != "0" {
		maxAgeInt, err := dur.ParseNDuration(maxAge)
		perror(err)
		cutoff = uint32(time.Now().Unix() - int64(maxAgeInt))
	}

	defs := idx.Load(nil, cutoff)
	// set this after doing the query, to assure age can't possibly be negative
	out.QueryTime = time.Now().Unix()
	total := len(defs)
	shown := 0

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
		if tags == "valid" || tags == "invalid" {
			valid := schema.ValidateTags(d.Tags)

			// skip the metric if the validation result is not what we want
			if valid != (tags == "valid") {
				continue
			}
		}
		show(d)
		shown += 1
		if shown == limit {
			break
		}
	}

	if verbose {
		fmt.Fprintf(os.Stderr, "total: %d\n", total)
		fmt.Fprintf(os.Stderr, "shown: %d\n", shown)
	}
}
