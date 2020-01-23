package main

import (
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"runtime"

	"github.com/grafana/globalconf"
	"github.com/grafana/metrictank/logger"
	log "github.com/sirupsen/logrus"
)

var (
	version       = "(none)"
	showVersion   = flag.Bool("version", false, "print version string")
	addr          = flag.String("addr", ":6059", "http service address")
	confFile      = flag.String("config", "/etc/metrictank/mt-gateway.ini", "configuration file path")
	metrictankUrl = flag.String("metrictank-url", "http://localhost:6060", "metrictank address")
	graphiteURL   = flag.String("graphite-url", "http://localhost:8080", "graphite-api address")
	importerURL   = flag.String("importer-url", "", "mt-whisper-importer-writer address")
	defaultOrgId  = flag.Int("default-org-id", -1, "default org ID to send to downstream services if none is provided")
	brokers       = flag.String("kafka-tcp-addr", "localhost:9092", "kafka tcp address(es) for metrics, in csv host[:port] format")
)

type Urls struct {
	metrictank   *url.URL
	graphite     *url.URL
	bulkImporter *url.URL
	kafkaBrokers string
}

func init() {
	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)
}

func main() {
	flag.Usage = func() {
		fmt.Println("mt-gateway")
		fmt.Println("Provides an HTTP gateway for interacting with metrictank, including metrics ingestion")
		fmt.Println()
		fmt.Println("Usage:")
		fmt.Println()
		fmt.Println("	mt-gateway [flags]")
		fmt.Println()
		fmt.Println("Flags:")
		flag.PrintDefaults()
	}

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
		log.Fatalf("error with configuration file: %s", err)
		os.Exit(1)
	}
	conf.ParseAll()

	if *showVersion {
		fmt.Printf("mt-gateway (version: %s - runtime: %s)\n", version, runtime.Version())
		return
	}

	urls := Urls{}
	urls.kafkaBrokers = *brokers

	urls.metrictank, err = url.Parse(*metrictankUrl)
	if err != nil {
		log.Fatal(err)
	}
	urls.graphite, err = url.Parse(*graphiteURL)
	if err != nil {
		log.Fatal(err)
	}
	urls.bulkImporter, err = url.Parse(*importerURL)
	if err != nil {
		log.Fatal(err)
	}

	log.WithField("addr", *addr).Info("starting server")
	log.WithError(http.ListenAndServe(*addr, NewApi(urls).Mux())).Fatal("terminating")
}
