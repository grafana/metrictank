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
	"github.com/grafana/metrictank/logger"
	"github.com/grafana/metrictank/stats"
	"github.com/raintank/schema"
	"github.com/raintank/schema/msg"
	"github.com/rakyll/globalconf"
	log "github.com/sirupsen/logrus"
)

var (
	confFile = flag.String("config", "/etc/metrictank/metrictank.ini", "configuration file path")
	formatMd = flag.String("format-md", "{{.Part}} {{.OrgId}} {{.Id}} {{.Name}} {{.Interval}} {{.Value}} {{.Time}} {{.Unit}} {{.Mtype}} {{.Tags}}", "template to render MetricData with")
	formatP  = flag.String("format-point", "{{.Part}} {{.MKey}} {{.Value}} {{.Time}}", "template to render MetricPoint data with")
	prefix   = flag.String("prefix", "", "only show metrics that have this prefix")
	substr   = flag.String("substr", "", "only show metrics that have this substring")
	invalid  = flag.Bool("invalid", false, "only show metrics that are invalid")

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

func dateInt64(ts int64) string {
	return time.Unix(ts, 0).Format(time.RFC3339)
}

func dateUint32(ts uint32) string {
	return dateInt64(int64(ts))
}

func newInputPrinter(formatMd, formatP string) inputPrinter {
	funcsMd := map[string]interface{}{
		"date": dateInt64,
	}
	funcsP := map[string]interface{}{
		"date": dateUint32,
	}

	tplMd := template.Must(template.New("format").Funcs(funcsMd).Parse(formatMd + "\n"))
	tplP := template.Must(template.New("format").Funcs(funcsP).Parse(formatP + "\n"))
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
	if *invalid {
		err := metric.Validate()
		if err == nil && metric.Time != 0 {
			return
		}
	}
	stdoutLock.Lock()
	err := ip.tplMd.Execute(os.Stdout, DataMd{
		partition,
		*metric,
	})

	stdoutLock.Unlock()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("executing template")
	}
}

func (ip inputPrinter) ProcessMetricPoint(point schema.MetricPoint, format msg.Format, partition int32) {

	if *invalid && point.Valid() {
		return
	}

	stdoutLock.Lock()
	err := ip.tplP.Execute(os.Stdout, DataP{
		partition,
		point,
	})
	stdoutLock.Unlock()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Error("executing template")
	}
}

func main() {
	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	formatter.QuoteEmptyFields = true

	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)

	flag.Usage = func() {
		fmt.Fprintln(os.Stderr, "mt-kafka-mdm-sniff")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintln(os.Stderr, "Inspects what's flowing through kafka (in mdm format) and reports it to you")
		fmt.Fprintf(os.Stderr, "\nFlags:\n\n")
		flag.PrintDefaults()
		fmt.Fprintln(os.Stderr, "you can also use functions in templates:")
		fmt.Fprintln(os.Stderr, "date: formats a unix timestamp as a date")
		fmt.Fprintln(os.Stderr, "example: mt-kafka-mdm-sniff -format-point '{{.Time | date}}'")
	}
	flag.Parse()
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
		log.WithFields(log.Fields{
			"error": err,
		}).Fatal("error with configuration file")
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
		log.WithFields(log.Fields{
			"signal": sig,
		}).Info("received signal. shutting down")
	case <-pluginFatal:
		log.Info("Mdm input plugin signalled a fatal error. Shutting down")
	}
	mdm.Stop()
}
