package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io/ioutil"
	"os"
	"os/exec"
	"runtime"

	"github.com/grafana/metrictank/pkg/logger"
	"github.com/grafana/metrictank/pkg/schema"
	log "github.com/sirupsen/logrus"
)

var (
	version     string
	showVersion = flag.Bool("version", false, "print version string")
)

func init() {
	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)
}

func main() {

	flag.Usage = func() {
		fmt.Println("mt-keygen")
		fmt.Println()
		fmt.Println("mt-keygen gives you the MKey for a specific MetricDefinition")
		fmt.Println("It fills a temp file with a template MetricDefinition")
		fmt.Println("It launches vim")
		fmt.Println("You fill in the important details - name / interval / tags /...")
		fmt.Println("It prints the MKey")
		fmt.Println()
		flag.PrintDefaults()
	}
	flag.Parse()

	if *showVersion {
		fmt.Printf("mt-keygen (version: %s - runtime: %s)\n", version, runtime.Version())
		return
	}
	d := schema.MetricDefinition{
		Unit:  "unknown",
		Mtype: "gauge",
	}

	var data []byte
	var err error
	var f *os.File

	if data, err = json.MarshalIndent(d, "", "    "); err != nil {
		log.Fatal(err)
	}
	if f, err = ioutil.TempFile("", "mt-gen-metricid"); err != nil {
		log.Fatal(err)
	}
	fmt.Println("using file", f.Name())
	defer os.Remove(f.Name())

	if _, err = f.Write(data); err != nil {
		log.Fatal(err)
	}
	if err := f.Close(); err != nil {
		log.Fatal(err)
	}
	cmd := exec.Command("vim", f.Name())
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err = cmd.Start(); err != nil {
		log.Fatal(err)
	}
	if err = cmd.Wait(); err != nil {
		log.Fatal(err)
	}

	data, err = ioutil.ReadFile(f.Name())
	if err != nil {
		log.Fatal(err)
	}
	if err = json.Unmarshal(data, &d); err != nil {
		log.Fatal(err)
	}

	d.SetId()
	fmt.Println(d.Id.String())
}
