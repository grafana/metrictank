package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/raintank/dur"
	"github.com/raintank/metrictank/expr"
)

func main() {
	stable := flag.Bool("stable", true, "whether to use only functionality marked as stable")
	from := flag.String("from", "-24h", "get data from (inclusive)")
	to := flag.String("to", "now", "get data until (exclusive)")
	mdp := flag.Int("mdp", 800, "max data points to return")

	flag.Usage = func() {
		fmt.Println("mt-explain")
		fmt.Println("Explains the execution plan for a given query / set of targets")
		fmt.Println()
		fmt.Printf("Usage:\n\n")
		fmt.Printf("  mt-explain\n")
		fmt.Println()
		fmt.Printf("Example:\n\n")
		fmt.Printf("  mt-explain -from -24h -to now -mdp 1000 \"movingAverage(sumSeries(foo.bar), '2min')\" \"alias(averageSeries(foo.*), 'foo-avg')\"\n\n")
	}

	flag.Parse()
	if flag.NArg() == 0 {
		log.Fatal("no target specified")
		os.Exit(-1)
	}
	targets := flag.Args()

	now := time.Now()
	defaultFrom := uint32(now.Add(-time.Duration(24) * time.Hour).Unix())
	defaultTo := uint32(now.Add(time.Duration(1) * time.Second).Unix())

	fromUnix, err := dur.ParseTSpec(*from, now, defaultFrom)
	if err != nil {
		log.Fatal(err)
	}

	toUnix, err := dur.ParseTSpec(*to, now, defaultTo)
	if err != nil {
		log.Fatal(err)
	}

	exps, err := expr.ParseMany(targets)
	if err != nil {
		fmt.Println("Error while parsing:", err)
		return
	}

	plan, err := expr.NewPlan(exps, fromUnix, toUnix, uint32(*mdp), *stable, nil)
	if err != nil {
		if fun, ok := err.(expr.ErrUnknownFunction); ok {
			fmt.Printf("Unsupported function %q: must defer query to graphite\n", string(fun))
			plan.Dump(os.Stdout)
			return
		}
		fmt.Println("Error while planning", err)
		return
	}
	plan.Dump(os.Stdout)
}
