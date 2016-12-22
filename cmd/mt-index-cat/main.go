package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"strconv"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/raintank/dur"
	"github.com/raintank/inspect/idx/cass"
	"github.com/raintank/inspect/inspect-idx/out"
	"gopkg.in/raintank/schema.v1"
)

func perror(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

func main() {

	var (
		addr   string
		from   string
		maxAge string
		count  bool
		old    bool

		total int
	)

	flag.StringVar(&addr, "addr", "http://localhost:6060", "graphite/metrictank address")
	flag.StringVar(&from, "from", "30min", "from. eg '30min', '5h', '14d', etc. or a unix timestamp")
	flag.StringVar(&maxAge, "max-age", "6h30min", "max age (last update diff with now) of metricdefs.  use 0 to disable")
	flag.BoolVar(&count, "count", false, "print number of metrics loaded to stderr")
	flag.BoolVar(&old, "old", false, "use old cassandra table, metric_def_idx. (prior to clustering)")

	flag.Usage = func() {
		fmt.Printf("%s by Dieter_be\n", os.Args[0])
		fmt.Println("Usage:")
		fmt.Printf("  inspect-idx [flags] idxtype host keyspace/index output \n")
		fmt.Printf("  idxtype cass: \n")
		fmt.Printf("    host: comma separated list of cassandra addresses in host:port form\n")
		fmt.Printf("    keyspace: cassandra keyspace\n")
		fmt.Printf("  idxtype es: not supported at this point\n")
		fmt.Printf("  output: dump|list|vegeta-render|vegeta-render-patterns\n")
		fmt.Println("Flags:")
		flag.PrintDefaults()
	}
	flag.Parse()
	if flag.NArg() != 4 {
		flag.Usage()
		os.Exit(-1)
	}
	args := flag.Args()
	var show func(d schema.MetricDefinition)

	switch args[3] {
	case "dump":
		show = out.Dump
	case "list":
		show = out.List
	case "vegeta-render":
		show = out.GetVegetaRender(addr, from)
	case "vegeta-render-patterns":
		show = out.GetVegetaRenderPattern(addr, from)
	default:
		log.Fatal("invalid output")
	}

	// from should either a unix timestamp, or a specification that graphite/metrictank will recognize.
	_, err := strconv.Atoi(from)
	if err != nil {
		_, err = dur.ParseUNsec(from)
		perror(err)
	}

	maxAgeInt := int64(0)
	if maxAge != "0" {
		var i uint32
		i, err = dur.ParseUNsec(maxAge)
		perror(err)
		maxAgeInt = int64(i)
	}

	if args[0] != "cass" {
		fmt.Fprintf(os.Stderr, "only cass supported at this point")
		flag.Usage()
		os.Exit(-1)
	}

	table := "metric_idx"
	if old {
		table = "metric_def_idx"
	}
	idx := cass.New(args[1], args[2], table)

	defs, err := idx.Get()
	perror(err)
	spew.Dump(defs)

	for _, d := range defs {
		if maxAgeInt != 0 && d.LastUpdate > time.Now().Unix()-maxAgeInt {
			total += 1
			show(d)
		}
	}

	if count {
		fmt.Fprintf(os.Stderr, "listed %d metrics\n", total)
	}
}
