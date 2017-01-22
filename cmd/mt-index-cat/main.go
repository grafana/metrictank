package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/raintank/dur"
	"github.com/raintank/metrictank/cmd/mt-index-cat/out"
	"github.com/raintank/metrictank/idx/cassandra"
	"gopkg.in/raintank/schema.v1"
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
	var from string
	var maxAge string
	var verbose bool
	var limit int

	globalFlags := flag.NewFlagSet("global config flags", flag.ExitOnError)
	globalFlags.StringVar(&addr, "addr", "http://localhost:6060", "graphite/metrictank address")
	globalFlags.StringVar(&prefix, "prefix", "", "only show metrics that have this prefix")
	globalFlags.StringVar(&substr, "substr", "", "only show metrics that have this substring")
	globalFlags.StringVar(&from, "from", "30min", "from. eg '30min', '5h', '14d', etc. or a unix timestamp")
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
		fmt.Printf("idxtype: only 'cass' supported for now\n\n")
		fmt.Printf("cass config flags:\n\n")
		cassFlags.PrintDefaults()
		fmt.Println()
		fmt.Printf("output: %v\n\n\n", strings.Join(outputs, "|"))
		fmt.Println("EXAMPLES:")
		fmt.Println("mt-index-cat -from 60min cass -hosts cassandra:9042 list")
	}

	if len(os.Args) == 2 && (os.Args[1] == "-h" || os.Args[1] == "--help") {
		flag.Usage()
		os.Exit(0)
	}

	if len(os.Args) < 3 {
		flag.Usage()
		os.Exit(-1)
	}

	last := os.Args[len(os.Args)-1]
	var found bool
	for _, output := range outputs {
		if last == output {
			found = true
			break
		}
	}
	if !found {
		log.Printf("invalid output %q", last)
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
	cassFlags.Parse(os.Args[cassI+1 : len(os.Args)-1])
	cassandra.Enabled = true

	var show func(d schema.MetricDefinition)

	switch os.Args[len(os.Args)-1] {
	case "dump":
		show = out.Dump
	case "list":
		show = out.List
	case "vegeta-render":
		show = out.GetVegetaRender(addr, from)
	case "vegeta-render-patterns":
		show = out.GetVegetaRenderPattern(addr, from)
	default:
		panic("this should never happen. we already validated the output type")
	}

	idx := cassandra.New()
	err := idx.InitBare()
	perror(err)

	// from should either be a unix timestamp, or a specification that graphite/metrictank will recognize.
	_, err = strconv.Atoi(from)
	if err != nil {
		_, err = dur.ParseUNsec(from)
		perror(err)
	}

	maxAgeInt := uint32(0)
	if maxAge != "0" {
		maxAgeInt, err = dur.ParseUNsec(maxAge)
		perror(err)
	}

	defs := idx.Load(nil)
	total := len(defs)
	shown := 0

	if maxAgeInt == 0 {
		for _, d := range defs {
			if prefix == "" || strings.HasPrefix(d.Metric, prefix) {
				if substr == "" || strings.Contains(d.Metric, substr) {
					show(d)
					shown += 1
					if shown == limit {
						break
					}
				}
			}
		}
	} else {
		cutoff := time.Now().Unix() - int64(maxAgeInt)
		for _, d := range defs {
			if d.LastUpdate > cutoff {
				if prefix == "" || strings.HasPrefix(d.Metric, prefix) {
					if substr == "" || strings.Contains(d.Metric, substr) {
						show(d)
						shown += 1
						if shown == limit {
							break
						}
					}
				}
			}
		}
	}

	if verbose {
		fmt.Fprintf(os.Stderr, "total: %d\n", total)
		fmt.Fprintf(os.Stderr, "shown: %d\n", shown)
	}
}
