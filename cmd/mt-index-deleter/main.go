package main

import (
	"bufio"
	"flag"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/grafana/metrictank/logger"
	log "github.com/sirupsen/logrus"
)

func init() {
	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)
}

func main() {
	project := flag.String("gcp-project", "default", "Name of GCP project the bigtable cluster resides in")
	instance := flag.String("bigtable-instance", "default", "Name of bigtable instance")
	tableName := flag.String("bigtable-table", "metric_idx", "Name of bigtable table used for metricDefs")
	concurrency := flag.Int("concurrency", 20, "number of concurrent delete workers")
	batchSize := flag.Int("batch", 10, "batch size of each delete")

	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s:\n", os.Args[0])
		fmt.Fprintln(flag.CommandLine.Output(), "reads rowkeys from stdin and deletes them from the index. only BigTable is supported right now")
		flag.PrintDefaults()
	}

	flag.Parse()

	btClient, err := NewBtClient(*project, *instance, *tableName)
	if err != nil {
		log.Fatal(err)
	}

	deletes := make(chan string, *concurrency*8)
	wg := &sync.WaitGroup{}

	wg.Add(*concurrency)

	workerStart = time.Now()

	for i := 0; i < *concurrency; i++ {
		go worker(wg, *btClient, *batchSize, deletes)
	}

	go func() {
		for {
			workerStats()
			time.Sleep(time.Minute)
		}
	}()

	read(os.Stdin, deletes)

	log.Infof("finished reading input. waiting for all workers to finish...")
	wg.Wait()
	log.Infof("all workers finished. exiting")
	workerStats()
}

func read(stream io.Reader, deletes chan string) {
	scanner := bufio.NewScanner(stream)
	for scanner.Scan() {
		rowKey := scanner.Text()
		deletes <- rowKey
	}
	if err := scanner.Err(); err != nil {
		log.Fatal(err)
	}
	close(deletes)
}
