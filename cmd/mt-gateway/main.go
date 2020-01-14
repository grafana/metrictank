package main

import (
	"flag"
	"fmt"
	"github.com/grafana/metrictank/logger"
	log "github.com/sirupsen/logrus"
	"net/http"
	"net/http/httputil"
	"net/url"
	"runtime"
)

var (
	version       = "(none)"
	showVersion   = flag.Bool("version", false, "print version string")
	metrictankUrl = flag.String("metrictank-url", "http://localhost:6060", "metrictank address")
	graphiteURL   = flag.String("graphite-url", "http://localhost:8080", "graphite-api address")
	importerURL   = flag.String("importer-url", "", "mt-whisper-importer-writer address")
	addr          = flag.String("addr", ":80", "http service address")
	urls          = Urls{}
)

type Urls struct {
	metrictank   *url.URL
	graphite     *url.URL
	bulkImporter *url.URL
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
	flag.Parse()

	if *showVersion {
		fmt.Printf("mt-gateway (version: %s - runtime: %s)\n", version, runtime.Version())
		return
	}
	var err error

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

	metrictankProxy := httputil.NewSingleHostReverseProxy(urls.metrictank)
	http.HandleFunc("/metrics/index.json", metrictankProxy.ServeHTTP)
	//TODO drop `/graphite` prefix... Or just drop this entry, do we really need the alias?
	http.HandleFunc("/graphite/metrics/index.json", metrictankProxy.ServeHTTP)
	http.HandleFunc("/metrics/delete", metrictankProxy.ServeHTTP)

	graphiteProxy := httputil.NewSingleHostReverseProxy(urls.graphite)
	//TODO drop `/graphite` prefix
	http.HandleFunc("/graphite/", graphiteProxy.ServeHTTP)

	bulkImporterProxy := httputil.NewSingleHostReverseProxy(urls.bulkImporter)
	//TODO figure out the final URL here
	http.HandleFunc("/metrics/import", bulkImporterProxy.ServeHTTP)

	//TODO implement kafka metrics ingest
	http.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusNotImplemented)
		fmt.Fprintln(w, "metrics ingest endpoint not yet implemented")

	})

	log.Fatal(http.ListenAndServe(*addr, nil))
}
