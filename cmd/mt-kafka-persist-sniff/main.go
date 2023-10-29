package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/grafana/metrictank/internal/mdata/notifierKafka"
	"github.com/grafana/metrictank/internal/stats"
	"github.com/grafana/metrictank/pkg/logger"
	log "github.com/sirupsen/logrus"
)

func main() {
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "mt-kafka-persist-sniff")
		fmt.Fprintln(os.Stderr, "Print what's flowing through kafka metric persist topic")
		fmt.Fprintf(os.Stderr, "\nFlags:\n\n")
		flag.PrintDefaults()
		notifierKafka.FlagSet.PrintDefaults()
	}
	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)
	instance := "mt-kafka-persist-sniff" + strconv.Itoa(rand.Int())

	notifierKafka.FlagSet.Usage = flag.Usage
	notifierKafka.FlagSet.Parse(os.Args[1:])
	// config may have had it disabled
	notifierKafka.Enabled = true

	stats.NewDevnull() // make sure metrics don't pile up without getting discarded

	notifierKafka.ConfigProcess(instance)

	done := make(chan struct{})
	go func() {
		notifierKafka.New(instance, NewPrintNotifierHandler())
		close(done)
	}()
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	select {
	case sig := <-sigChan:
		log.Infof("Received signal %q. Shutting down", sig)
	case <-done:
	}
	//	notifierKafka.Stop()
}
