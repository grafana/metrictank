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

	inKafkaMdm "github.com/grafana/metrictank/input/kafkamdm"
	"github.com/grafana/metrictank/stats"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"
	"gopkg.in/raintank/schema.v1"
)

var (
	confFile = flag.String("config", "/etc/metrictank/metrictank.ini", "configuration file path")
	formatMd = flag.String("format-md", "{{.Part}} {{.OrgId}} {{.Id}} {{.Name}} {{.Interval}} {{.Value}} {{.Time}} {{.Unit}} {{.Mtype}} {{.Tags}}", "template to render MetricData with")
	formatP  = flag.String("format-point", "{{.Part}} {{.MKey}} {{.Value}} {{.Time}}", "template to render MetricPoint data with")
	prefix   = flag.String("prefix", "", "only show metrics that have this prefix")
	substr   = flag.String("substr", "", "only show metrics that have this substring")

	stdoutLock = sync.Mutex{}
)

type DataMd struct {
	Part int32
	schema.MetricData
}

type DataP struct {
	Part int32
	schema.MetricPoint
}

type inputPrinter struct {
	tplMd template.Template
	tplP  template.Template
}

func newInputPrinter(formatMd, formatP string) inputPrinter {
	tplMd := template.Must(template.New("format").Parse(formatMd + "\n"))
	tplP := template.Must(template.New("format").Parse(formatP + "\n"))
	return inputPrinter{
		*tplMd,
		*tplP,
	}
}

func (ip inputPrinter) ProcessMetricData(metric *schema.MetricData, partition int32) {
	if *prefix != "" && !strings.HasPrefix(metric.Name, *prefix) {
		return
	}
	if *substr != "" && !strings.Contains(metric.Name, *substr) {
		return
	}
	stdoutLock.Lock()
	err := ip.tplMd.Execute(os.Stdout, DataMd{
		partition,
		*metric,
	})

	stdoutLock.Unlock()
	if err != nil {
		log.Error(0, "executing template: %s", err)
	}
}

func (ip inputPrinter) ProcessMetricPoint(point schema.MetricPoint, partition int32) {
	stdoutLock.Lock()
	err := ip.tplP.Execute(os.Stdout, DataP{
		partition,
		point,
	})
	stdoutLock.Unlock()
	if err != nil {
		log.Error(0, "executing template: %s", err)
	}
}

func main() {
	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "mt-kafka-mdm-sniff")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Inspects what's flowing through kafka (in mdm format) and reports it to you")
		fmt.Fprintf(os.Stderr, "\nFlags:\n\n")
		flag.PrintDefaults()
	}
	flag.Parse()
	log.NewLogger(0, "console", fmt.Sprintf(`{"level": %d, "formatting":false}`, 2))
	instance := "mt-kafka-mdm-sniff" + strconv.Itoa(rand.Int())

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
	pluginFatal := make(chan struct{})
	mdm.Start(newInputPrinter(*formatMd, *formatP), pluginFatal)
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	select {
	case sig := <-sigChan:
		log.Info("Received signal %q. Shutting down", sig)
	case <-pluginFatal:
		log.Info("Mdm input plugin signalled a fatal error. Shutting down")
	}
	mdm.Stop()
}
