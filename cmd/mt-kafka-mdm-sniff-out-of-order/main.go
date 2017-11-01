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
	format   = flag.String("format", "{{.OOO.Id}} {{.OOO.Name}} {{.DeltaTime}} {{.DeltaSeen}} {{.NumOOO}}", "template to render event with")
	prefix   = flag.String("prefix", "", "only show metrics with a name that has this prefix")
	substr   = flag.String("substr", "", "only show metrics with a name that has this substring")
)

type Tracker struct {
	Head      Msg   // last successfully added message
	OOO       Msg   // current point that could not be added (assuming no re-order buffer)
	NumOOO    int   // number of failed points since last successfull add
	DeltaTime int64 // delta between Head and OOO time properties in seconds (point timestamps)
	DeltaSeen int64 // delta between Head and OOO seen time in seconds (consumed from kafka)
}

type Msg struct {
	Part int32
	Seen time.Time
	schema.MetricData
}

// find out of order metrics
type inputOOOFinder struct {
	template.Template
	data map[string]Tracker // by metric id
	lock sync.Mutex
}

func newInputOOOFinder(format string) *inputOOOFinder {
	tpl := template.Must(template.New("format").Parse(format + "\n"))
	return &inputOOOFinder{
		*tpl,
		make(map[string]Tracker),
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
	now := Msg{
		Part:       partition,
		Seen:       time.Now(),
		MetricData: *metric,
	}
	ip.lock.Lock()
	tracker, ok := ip.data[metric.Id]
	if !ok {
		ip.data[metric.Id] = Tracker{
			Head: now,
		}
	} else {
		if metric.Time > tracker.Head.Time {
			tracker.Head = now
			tracker.NumOOO = 0
			ip.data[metric.Id] = tracker
		} else {
			// if metric time <= head point time, generate event and print
			tracker.OOO = now
			tracker.NumOOO += 1
			tracker.DeltaTime = tracker.Head.Time - metric.Time
			tracker.DeltaSeen = now.Seen.Unix() - tracker.Head.Seen.Unix()
			err := ip.Execute(os.Stdout, tracker)
			if err != nil {
				log.Error(0, "executing template: %s", err)
			}
			ip.data[metric.Id] = tracker
		}
	}
	ip.lock.Unlock()
}

func main() {
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "mt-kafka-mdm-sniff-out-of-order")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Inspects what's flowing through kafka (in mdm format) and reports out of order data (does not take into account reorder buffer)")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "# Mechanism")
		fmt.Fprintln(os.Stderr, "* it sniffs points being added on a per-series (metric Id) level")
		fmt.Fprintln(os.Stderr, "* for every series, tracks the last 'correct' point.  E.g. a point that was able to be added to the series because its timestamp is higher than any previous timestamp")
		fmt.Fprintln(os.Stderr, "* if for any series, a point comes in with a timestamp equal or lower than the last point correct point - which metrictank would not add unless it falls within the reorder buffer - it triggers an event for this out-of-order point")
		fmt.Fprintln(os.Stderr, "every event is printed using the specified format")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "# Event formatting")
		fmt.Fprintln(os.Stderr, "Uses standard golang templating. E.g. {{field}} with these available fields:")
		fmt.Fprintln(os.Stderr, ".Head.subfield - head is last successfully added message")
		fmt.Fprintln(os.Stderr, ".OOO.subfield - OOO is the current point that could not be added (assuming no re-order buffer)")
		fmt.Fprintln(os.Stderr, "(subfield is any property of the out-of-order MetricData: Time OrgId Id Name Metric Interval Value Unit Mtype Tags and also these 2 extra fileds: Part (partition) and Seen (when the msg was consumed from kafka)")
		fmt.Fprintln(os.Stderr, "NumOOO - number of failed points since last successfull add")
		fmt.Fprintln(os.Stderr, "DeltaTime - delta between Head and OOO time properties in seconds (point timestamps)")
		fmt.Fprintln(os.Stderr, "DeltaSeen - delta between Head and OOO seen time in seconds (consumed from kafka)")
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
