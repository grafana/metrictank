package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/grafana/globalconf"
	log "github.com/sirupsen/logrus"
)

var (
	producer *Producer
	cass     *Cassandra

	confFile = flag.String("config", "", "configuration file path (optional)")
)

func main() {
	flag.Usage = func() {
		fmt.Println("mt-control-server")
		fmt.Println()
		fmt.Println("Run a control server that can be used to issue control messages to a metrictank cluster.")
		fmt.Println("Flags:")
		flag.PrintDefaults()
	}
	flag.Parse()

	config, err := globalconf.NewWithOptions(&globalconf.Options{
		Filename:  *confFile,
		EnvPrefix: "MT_",
	})
	if err != nil {
		fmt.Fprintf(os.Stderr, "FATAL: configuration file error: %s", err)
		os.Exit(1)
	}

	ConfigServer()
	ConfigKafka()
	ConfigCassandra()
	ConfigHandlers()

	config.ParseAll()

	apiServer, err := NewServer()
	if err != nil {
		log.Fatalf("Failed to start API. %s", err.Error())
	}

	producer = NewProducer()
	cass, err = NewCassandra()
	if err != nil {
		log.Fatalf("Failed to config cassandra. %s", err.Error())
	}

	apiServer.Run()
	// SEAN TODO - handle shutdown somehow
}
