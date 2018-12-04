package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/grafana/metrictank/logger"
	"github.com/grafana/metrictank/mdata/notifierKafka"
	"github.com/grafana/metrictank/stats"
	"github.com/rakyll/globalconf"
	log "github.com/sirupsen/logrus"
)

var confFile = flag.String("config", "/etc/metrictank/metrictank.ini", "configuration file path")

func main() {
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "mt-kafka-persist-sniff")
		fmt.Fprintln(os.Stderr, "Print what's flowing through kafka metric persist topic")
		fmt.Fprintf(os.Stderr, "\nFlags:\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)
	instance := "mt-kafka-persist-sniff" + strconv.Itoa(rand.Int())

	// Only try and parse the conf file if it exists
	path := ""
	if _, err := os.Stat(*confFile); err == nil {
		path = *confFile
	}
	conf, err := globalconf.NewWithOptions(&globalconf.Options{
		Filename:  path,
		EnvPrefix: "MT_",
	})
	if err != nil {
		log.Fatalf("error with configuration file: %s", err.Error())
		os.Exit(1)
	}
	conf.ParseAll()

	// config may have had it disabled
	notifierKafka.Enabled = true

	stats.NewDevnull() // make sure metrics don't pile up without getting discarded

	notifierKafka.ConfigProcess(instance)
	notifierKafka.New(instance, NewPrintNotifierHandler())
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigChan
	log.Infof("Received signal %q. Shutting down", sig)
	//	notifierKafka.Stop()
}
