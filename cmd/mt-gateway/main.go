package main

import (
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"runtime"
	"strings"
	"time"

	"github.com/grafana/globalconf"
	"github.com/grafana/metrictank/logger"
	"github.com/grafana/metrictank/stats"
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
	logLevel      = flag.String("log-level", "info", "log level. panic|fatal|error|warning|info|debug")

	// stats
	statsEnabled    = flag.Bool("stats-enabled", false, "enable sending graphite messages for instrumentation")
	statsPrefix     = flag.String("stats-prefix", "mt-gateway.stats.default.$hostname", "stats prefix (will add trailing dot automatically if needed)")
	statsAddr       = flag.String("stats-addr", "localhost:2003", "graphite address")
	statsInterval   = flag.Int("stats-interval", 10, "interval in seconds to send statistics")
	statsBufferSize = flag.Int("stats-buffer-size", 20000, "how many messages (holding all measurements from one interval) to buffer up in case graphite endpoint is unavailable.")
	statsTimeout    = flag.Duration("stats-timeout", time.Second*10, "timeout after which a write is considered not successful")
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
}

func main() {
	flag.Usage = func() {
		fmt.Println("mt-gateway")
		fmt.Println("Provides an HTTP gateway for interacting with Grafana Metrictank, including metrics ingestion")
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
	}
	conf.ParseAll()

	lvl, err := log.ParseLevel(*logLevel)
	if err != nil {
		log.Fatalf("failed to parse log-level, %s", err.Error())
	}
	log.SetLevel(lvl)

	if *showVersion {
		fmt.Printf("mt-gateway (version: %s - runtime: %s)\n", version, runtime.Version())
		return
	}

	if *statsEnabled {
		stats.NewMemoryReporter()
		hostname, _ := os.Hostname()
		prefix := strings.Replace(*statsPrefix, "$hostname", strings.Replace(hostname, ".", "_", -1), -1)
		stats.NewGraphite(prefix, *statsAddr, *statsInterval, *statsBufferSize, *statsTimeout)
	} else {
		stats.NewDevnull()
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
