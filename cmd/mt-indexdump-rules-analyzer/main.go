package main

import (
	"bufio"
	"context"
	"flag"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gosuri/uilive"
	"github.com/grafana/metrictank/conf"
	"github.com/grafana/metrictank/logger"
	log "github.com/sirupsen/logrus"
)

func init() {
	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)
}

func perror(err error) {
	if err != nil {
		log.Fatal(err.Error())
	}
}

func showStats(indexRules conf.IndexRules, counts []uint64, wg *sync.WaitGroup, ctx context.Context) {

	start := time.Now()

	writer := uilive.New()
	writer.Start()

	printTable := func() {
		fmt.Fprintln(writer, "Count        RuleID Pattern")
		for i := range counts {
			fmt.Fprintf(writer, "%12d %6d %s\n", atomic.LoadUint64(&counts[i]), i, indexRules.Get(uint16(i)))
		}
	}

	tick := time.NewTicker(time.Second)
	for {
		select {
		case <-tick.C:
			printTable()
			writer.Flush()
		case <-ctx.Done():
			printTable()
			fmt.Fprintf(writer, "Finished in %v\n", time.Since(start))
			writer.Stop()
			wg.Done()
			return
		}
	}

}

func main() {
	var indexRulesFile string
	flag.StringVar(&indexRulesFile, "index-rules-file", "/etc/metrictank/index-rules.conf", "name of file which defines the max-stale times")
	flag.Parse()

	indexRules, err := conf.ReadIndexRules(indexRulesFile)
	if os.IsNotExist(err) {
		log.Fatalf("Index-rules.conf file %s does not exist; exiting", indexRulesFile)
		os.Exit(1)
	}
	scanner := bufio.NewScanner(os.Stdin)

	counts := make([]uint64, len(indexRules.Rules)+1)

	ctx, cancel := context.WithCancel(context.Background())
	wg := &sync.WaitGroup{}

	wg.Add(1)
	go showStats(indexRules, counts, wg, ctx)

	for scanner.Scan() {
		word := scanner.Text()
		ruleID, _ := indexRules.Match(word)
		atomic.AddUint64(&counts[ruleID], 1)
	}
	cancel()
	wg.Wait()

	if err := scanner.Err(); err != nil {
		log.Println(err)
	}

}
