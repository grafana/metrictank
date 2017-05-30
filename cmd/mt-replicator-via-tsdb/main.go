package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"syscall"

	"github.com/raintank/worldping-api/pkg/log"
)

var (
	GitHash     = "(none)"
	showVersion = flag.Bool("version", false, "print version string")
	logLevel    = flag.Int("log-level", 2, "log level. 0=TRACE|1=DEBUG|2=INFO|3=WARN|4=ERROR|5=CRITICAL|6=FATAL")
)

func main() {
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "mt-replicator-via-tsdb")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Replicates a kafka mdm topic on a given cluster to a remote tsdb-gw server")
		fmt.Fprintf(os.Stderr, "\nFlags:\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	log.NewLogger(0, "console", fmt.Sprintf(`{"level": %d, "formatting":false}`, *logLevel))

	if *showVersion {
		fmt.Printf("mt-replicator-via-tsdb (built with %s, git hash %s)\n", runtime.Version(), GitHash)
		return
	}

	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)

	metrics, err := NewMetricsReplicator()
	if err != nil {
		log.Fatal(4, err.Error())
	}

	log.Info("starting metrics replicator")
	metrics.Start()

	<-sigChan
	log.Info("metrics replicator shutdown started.")
	metrics.Stop()
	log.Info("shutdown complete")
	log.Close()
}
