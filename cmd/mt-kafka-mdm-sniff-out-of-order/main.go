package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"text/template"
	"time"

	inKafkaMdm "github.com/grafana/metrictank/input/kafkamdm"
	"github.com/grafana/metrictank/stats"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"
	"gopkg.in/raintank/schema.v1"
)

var (
	confFile = flag.String("config", "/etc/metrictank/metrictank.ini", "configuration file path")
	format   = flag.String("format", "{{.First.Seen}} {{.First.Time}} | {{.Seen}} {{.Time}} {{.Part}} {{.OrgId}} {{.Id}} {{.Name}} {{.Metric}} {{.Interval}} {{.Value}} {{.Unit}} {{.Mtype}} {{.Tags}}", "template to render the data with")
	prefix   = flag.String("prefix", "", "only show metrics that have this prefix")
	substr   = flag.String("substr", "", "only show metrics that have this substring")
)

type Data struct {
	Part int32
	Seen time.Time
	schema.MetricData
}

type TplData struct {
	Data       // currently seen
	First Data // seen the first time
}

// find out of order metrics
type inputOOOFinder struct {
	template.Template
	data map[string]Data // by metric name
	lock sync.Mutex
}

func newInputOOOFinder(format string) *inputOOOFinder {
	tpl := template.Must(template.New("format").Parse(format + "\n"))
	return &inputOOOFinder{
		*tpl,
		make(map[string]Data),
		sync.Mutex{},
	}
}

func (ip *inputOOOFinder) Process(metric *schema.MetricData, partition int32) {
	if *prefix != "" && !strings.HasPrefix(metric.Metric, *prefix) {
		return
	}
	if *substr != "" && !strings.Contains(metric.Metric, *substr) {
		return
	}
	now := Data{
		Part:       partition,
		Seen:       time.Now(),
		MetricData: *metric,
	}
	ip.lock.Lock()
	first, ok := ip.data[metric.Name]
	if !ok {
		ip.data[metric.Name] = now
	} else {
		if metric.Time > first.Time {
			ip.data[metric.Name] = now
		} else {
			t := TplData{now, first}
			err := ip.Execute(os.Stdout, t)
			if err != nil {
				log.Error(0, "executing template: %s", err)
			}
		}
	}
	ip.lock.Unlock()
}

func main() {
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "mt-kafka-mdm-sniff-out-of-order")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Inspects what's flowing through kafka (in mdm format) and reports it to you")
		fmt.Fprintf(os.Stderr, "\nFlags:\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	log.NewLogger(0, "console", fmt.Sprintf(`{"level": %d, "formatting":false}`, 2))
	instance := "mt-kafka-mdm-sniff-out-of-order" + strconv.Itoa(rand.Int())

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
		log.Fatal(4, "error with configuration file: %s", err)
		os.Exit(1)
	}
	inKafkaMdm.ConfigSetup()
	conf.ParseAll()

	// config may have had it disabled
	inKafkaMdm.Enabled = true
	// important: we don't want to share the same offset tracker as the mdm input of MT itself
	inKafkaMdm.DataDir = "/tmp/" + instance

	inKafkaMdm.ConfigProcess(instance)

	stats.NewDevnull() // make sure metrics don't pile up without getting discarded

	mdm := inKafkaMdm.New()
	mdm.Start(newInputOOOFinder(*format))
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	log.Info("stopping")
	mdm.Stop()
}
