package main

import (
	"bufio"
	"flag"
	"fmt"
	"os"
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

	writer := uilive.New()
	writer.Start()

	go func() {
		tick := time.NewTicker(100 * time.Millisecond)
		for range tick.C {
			for i := range counts {
				fmt.Fprintf(writer, "Patt [%d]: %s    -- %d\n", i, indexRules.Get(uint16(i)), atomic.LoadUint64(&counts[i]))
			}
			writer.Flush()
		}

	}()

	for scanner.Scan() {
		word := scanner.Text()
		ruleID, _ := indexRules.Match(word)
		atomic.AddUint64(&counts[ruleID], 1)
	}
	writer.Stop()

	if err := scanner.Err(); err != nil {
		log.Println(err)
	}

}
