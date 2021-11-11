package main

import (
	"context"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"
	"time"

	inKafkaMdm "github.com/grafana/metrictank/input/kafkamdm"
	"github.com/grafana/metrictank/logger"
	log "github.com/sirupsen/logrus"
)

func configureLogging() {
	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)
}

func filter(tracker Tracker, prefix string, substr string) {
	for key, track := range tracker {
		if prefix != "" && !strings.HasPrefix(track.Name, prefix) {
			delete(tracker, key)
		}
		if substr != "" && !strings.Contains(track.Name, substr) {
			delete(tracker, key)
		}
	}
}

func asPercent(numerator int, denominator int) float32 {
	return float32(numerator) / float32(denominator) * 100
}

func aggregateAndLog(tracker Tracker, groupByName bool, groupByTag string) {
	count := 0
	outOfOrderCount := 0
	duplicateCount := 0
	for _, track := range tracker {
		count += track.Count
		outOfOrderCount += track.OutOfOrderCount
		duplicateCount += track.DuplicateCount
	}

	var aggregatedByName map[string]Aggregate
	if groupByName {
		aggregatedByName = aggregateByName(tracker)
	}

	var aggregatedByTag map[string]Aggregate
	if groupByTag != "" {
		aggregatedByTag = aggregateByTag(tracker, groupByTag)
	}

	log.Infof("total metric points count=%d", count)

	log.Infof("total out-of-order metric points count=%d", outOfOrderCount)
	if groupByName && outOfOrderCount > 0 {
		log.Info("out-of-order metric points grouped by name:")
		for name, aggregate := range aggregatedByName {
			if aggregate.OutOfOrderCount > 0 {
				log.Infof("out-of-order metric points for name=%q count=%d percentGroup=%f percentClass=%f percentTotal=%f", name, aggregate.OutOfOrderCount, asPercent(aggregate.OutOfOrderCount, aggregate.Count), asPercent(aggregate.OutOfOrderCount, outOfOrderCount), asPercent(aggregate.OutOfOrderCount, count))
			}
		}
	}
	if groupByTag != "" && outOfOrderCount > 0 {
		log.Infof("out-of-order metric points grouped by tag=%q:", groupByTag)
		for tag, aggregate := range aggregatedByTag {
			if aggregate.OutOfOrderCount > 0 {
				log.Infof("out-of-order metric points for tag=%q value=%q count=%d percentGroup=%f percentClass=%f percentTotal=%f", groupByTag, tag, aggregate.OutOfOrderCount, asPercent(aggregate.OutOfOrderCount, aggregate.Count), asPercent(aggregate.OutOfOrderCount, outOfOrderCount), asPercent(aggregate.OutOfOrderCount, count))
			}
		}
	}

	log.Infof("total duplicate metric points count=%d", duplicateCount)
	if groupByName && duplicateCount > 0 {
		log.Info("duplicate metric points grouped by name:")
		for name, aggregate := range aggregatedByName {
			if aggregate.DuplicateCount > 0 {
				log.Infof("duplicate metric points for name=%q count=%d percentGroup=%f percentClass=%f percentTotal=%f", name, aggregate.DuplicateCount, asPercent(aggregate.DuplicateCount, aggregate.Count), asPercent(aggregate.DuplicateCount, duplicateCount), asPercent(aggregate.DuplicateCount, count))
			}
		}
	}
	if groupByTag != "" && duplicateCount > 0 {
		log.Infof("duplicate metric points grouped by tag=%q:", groupByTag)
		for tag, aggregate := range aggregatedByTag {
			if aggregate.DuplicateCount > 0 {
				log.Infof("duplicate metric points for tag=%q value=%q count=%d percentGroup=%f percentClass=%f percentTotal=%f", groupByTag, tag, aggregate.DuplicateCount, asPercent(aggregate.DuplicateCount, aggregate.Count), asPercent(aggregate.DuplicateCount, duplicateCount), asPercent(aggregate.DuplicateCount, count))
			}
		}
	}
}

func main() {
	configureLogging()

	flags := ParseFlags()

	inKafkaMdm.ConfigProcess("mt-kafka-mdm-report-out-of-order" + strconv.Itoa(rand.Int()))
	kafkaMdm := inKafkaMdm.New()

	inputOOOFinder := newInputOOOFinder(
		flags.PartitionFrom,
		flags.PartitionTo,
		uint32(flags.ReorderWindow),
	)

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	ctx, cancel := context.WithCancel(context.Background())
	kafkaMdm.Start(inputOOOFinder, cancel)
	select {
	case sig := <-sigChan:
		log.Infof("Received signal %q. Shutting down", sig)
	case <-ctx.Done():
		log.Info("Mdm input plugin signalled a fatal error. Shutting down")
	case <-time.After(flags.RunDuration):
		log.Infof("Finished scanning")
	}
	kafkaMdm.Stop()

	tracker := inputOOOFinder.Tracker()
	filter(tracker, flags.Prefix, flags.Substr)
	aggregateAndLog(tracker, flags.GroupByName, flags.GroupByTag)
}
