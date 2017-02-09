package main

import (
	"flag"
	"fmt"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"
	"text/template"

	inKafkaMdm "github.com/raintank/metrictank/input/kafkamdm"
	"github.com/raintank/metrictank/stats"
	"github.com/raintank/worldping-api/pkg/log"
	"github.com/rakyll/globalconf"
	"gopkg.in/raintank/schema.v1"
)

var (
	confFile = flag.String("config", "/etc/raintank/metrictank.ini", "configuration file path")
	format   = flag.String("format", "{{.Part}} {{.OrgId}} {{.Id}} {{.Name}} {{.Metric}} {{.Interval}} {{.Value}} {{.Time}} {{.Unit}} {{.Mtype}} {{.Tags}}", "template to render the data with")

	stdoutLock = sync.Mutex{}
)

type Data struct {
	Part int32
	schema.MetricData
}

type inputPrinter struct {
	template.Template
	data Data
}

func newInputPrinter(format string) inputPrinter {
	tpl := template.Must(template.New("format").Parse(format + "\n"))
	return inputPrinter{
		*tpl,
		Data{},
	}
}

func (ip inputPrinter) Process(metric *schema.MetricData, partition int32) {
	ip.data.MetricData = *metric
	ip.data.Part = partition
	stdoutLock.Lock()
	err := ip.Execute(os.Stdout, ip.data)
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
	mdm.Start(newInputPrinter(*format))
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, syscall.SIGINT, syscall.SIGTERM)
	<-sigChan
	log.Info("stopping")
	mdm.Stop()
}
