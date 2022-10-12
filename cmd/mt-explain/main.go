package main

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/grafana/metrictank/pkg/expr"
	"github.com/grafana/metrictank/pkg/logger"
	"github.com/raintank/dur"
	log "github.com/sirupsen/logrus"
)

func init() {
	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)
}

func main() {
	stable := flag.Bool("stable", true, "whether to use only functionality marked as stable")
	from := flag.String("from", "-24h", "get data from (inclusive)")
	to := flag.String("to", "now", "get data until (exclusive)")
	mdp := flag.Int("mdp", 800, "max data points to return")
	timeZoneStr := flag.String("time-zone", "local", "time-zone to use for interpreting from/to when needed. (check your config)")
	var optimizations expr.Optimizations
	flag.BoolVar(&optimizations.PreNormalization, "pre-normalization", true, "enable pre-normalization optimization")
	flag.BoolVar(&optimizations.MDP, "mdp-optimization", false, "enable MaxDataPoints optimization (experimental)")

	flag.Usage = func() {
		fmt.Println("mt-explain")
		fmt.Println("Explains the execution plan for a given query / set of targets")
		fmt.Println()
		fmt.Printf("Usage:\n\n")
		fmt.Printf("  mt-explain\n")
		flag.PrintDefaults()
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

	var loc *time.Location
	switch *timeZoneStr {
	case "local":
		loc = time.Local
	default:
		var err error
		loc, err = time.LoadLocation(*timeZoneStr)
		if err != nil {
			log.Fatal(err.Error())
		}
	}

	now := time.Now()
	defaultFrom := uint32(now.Add(-time.Duration(24) * time.Hour).Unix())
	defaultTo := uint32(now.Add(time.Duration(1) * time.Second).Unix())

	fromUnix, err := dur.ParseDateTime(*from, loc, now, defaultFrom)
	if err != nil {
		log.Fatal(err.Error())
	}

	toUnix, err := dur.ParseDateTime(*to, loc, now, defaultTo)
	if err != nil {
		log.Fatal(err.Error())
	}

	exps, err := expr.ParseMany(targets)
	if err != nil {
		fmt.Println("Error while parsing:", err)
		return
	}

	plan, err := expr.NewPlan(exps, fromUnix, toUnix, uint32(*mdp), *stable, optimizations)
	if err != nil {
		if fun := expr.ErrUnknownFunction(""); errors.As(err, &fun) {
			fmt.Printf("Unsupported function %q: must defer query to graphite\n", string(fun))
			plan.Dump(os.Stdout)
			return
		}
		fmt.Println("Error while planning", err)
		return
	}
	plan.Dump(os.Stdout)
}
