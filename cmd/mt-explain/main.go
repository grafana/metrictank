package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/grafana/metrictank/expr"
	"github.com/grafana/metrictank/logger"
	"github.com/raintank/dur"
	log "github.com/sirupsen/logrus"
)

func main() {
	stable := flag.Bool("stable", true, "whether to use only functionality marked as stable")
	from := flag.String("from", "-24h", "get data from (inclusive)")
	to := flag.String("to", "now", "get data until (exclusive)")
	mdp := flag.Int("mdp", 800, "max data points to return")
	timeZoneStr := flag.String("time-zone", "local", "time-zone to use for interpreting from/to when needed. (check your config)")

	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	formatter.QuoteEmptyFields = true

	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)

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
			log.WithFields(log.Fields{
				"error":     err.Error(),
				"time.zone": *timeZoneStr,
			}).Fatal("failed to load time zone")
		}
	}

	now := time.Now()
	defaultFrom := uint32(now.Add(-time.Duration(24) * time.Hour).Unix())
	defaultTo := uint32(now.Add(time.Duration(1) * time.Second).Unix())

	fromUnix, err := dur.ParseDateTime(*from, loc, now, defaultFrom)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Fatal("failed to parse date and time")
	}

	toUnix, err := dur.ParseDateTime(*to, loc, now, defaultTo)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Fatal("failed to parse date and time")
	}

	exps, err := expr.ParseMany(targets)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("error while parsing")
		return
	}

	plan, err := expr.NewPlan(exps, fromUnix, toUnix, uint32(*mdp), *stable, nil)
	if err != nil {
		if fun, ok := err.(expr.ErrUnknownFunction); ok {
			log.WithFields(log.Fields{
				"function": string(fun),
			}).Info("unsupported function, must defer query to graphite")
			plan.Dump(os.Stdout)
			return
		}
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Error("error while planning")
		return
	}
	plan.Dump(os.Stdout)
}
