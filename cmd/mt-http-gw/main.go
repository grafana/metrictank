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
	metrictankUrl = flag.String("metrictank-url", "", "the url of the metrictank instance to proxy requests to")
	bindAddress   = flag.String("bind-address", ":8080", "the address to bind to")
)

func init() {
	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)
}

func main() {
	flag.Usage = func() {
		fmt.Println("mt-http-gw")
		fmt.Println()
		fmt.Println("Usage:")
		fmt.Println()
		fmt.Println("	mt-http-gw [flags]")
		fmt.Println()
		fmt.Println("Flags:")
		flag.PrintDefaults()
	}
	flag.Parse()

	if *showVersion {
		fmt.Printf("mt-http-gw (version: %s - runtime: %s)\n", version, runtime.Version())
		return
	}

	mtUrl, err := url.Parse(*metrictankUrl)
	if err != nil {
		log.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(mtUrl)

	http.HandleFunc("/metrics/index.json", proxy.ServeHTTP)
	log.Fatal(http.ListenAndServe(*bindAddress, nil))
}
