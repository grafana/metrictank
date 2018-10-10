package end2end_carbon_bigtable

import (
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/grafana/metrictank/logger"
	"github.com/grafana/metrictank/stacktest/docker"
	"github.com/grafana/metrictank/stacktest/fakemetrics"
	"github.com/grafana/metrictank/stacktest/grafana"
	"github.com/grafana/metrictank/stacktest/graphite"
	"github.com/grafana/metrictank/stacktest/track"
	log "github.com/sirupsen/logrus"
)

// TODO: cleanup when ctrl-C go test (teardown all containers)

var tracker *track.Tracker
var fm *fakemetrics.FakeMetrics

const metricsPerSecond = 1000

func init() {
	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)
}
func TestMain(m *testing.M) {
	log.Println("launching docker-dev-bigtable stack...")
	version := exec.Command("docker-compose", "version")
	output, err := version.CombinedOutput()
	if err != nil {
		log.Fatal(err.Error())
	}
	log.Println(string(output))

	// TODO: should probably use -V flag here.
	// introduced here https://github.com/docker/compose/releases/tag/1.19.0
	// but circleCI machine image still stuck with 1.14.0
	cmd := exec.Command("docker-compose", "up", "--force-recreate")
	cmd.Dir = docker.Path("docker/docker-dev-bigtable")

	tracker, err = track.NewTracker(cmd, false, false, "launch-stdout", "launch-stderr")
	if err != nil {
		log.Fatal(err.Error())
	}

	err = cmd.Start()
	if err != nil {
		log.Fatal(err.Error())
	}

	retcode := m.Run()
	fm.Close()

	log.Println("stopping docker-compose stack...")
	cmd.Process.Signal(syscall.SIGINT)
	// note: even when we don't care about the output, it's best to consume it before calling cmd.Wait()
	// even though the cmd.Wait docs say it will wait for stdout/stderr copying to complete
	// however the docs for cmd.StdoutPipe say "it is incorrect to call Wait before all reads from the pipe have completed"
	tracker.Wait()
	err = cmd.Wait()

	// 130 means ctrl-C (interrupt) which is what we want
	if err != nil && err.Error() != "exit status 130" {
		log.Printf("ERROR: could not cleanly shutdown running docker-compose command: %s", err)
		retcode = 1
	} else {
		log.Println("docker-compose stack is shut down")
	}

	os.Exit(retcode)
}

func TestStartup(t *testing.T) {
	matchers := []track.Matcher{
		{Str: "metrictank.*metricIndex initialized.*starting data consumption$"},
		{Str: "metrictank.*carbon-in: listening on.*2003"},
	}
	select {
	case <-tracker.Match(matchers):
		log.Println("stack now running.")
		log.Println("Go to http://localhost:3000 (and login as admin:admin) to see what's going on")
	case <-time.After(time.Second * 70):
		grafana.PostAnnotation("TestStartup:FAIL")
		t.Fatal("timed out while waiting for all metrictank instances to come up")
	}
}

func TestBaseIngestWorkload(t *testing.T) {
	grafana.PostAnnotation("TestBaseIngestWorkload:begin")

	fm = fakemetrics.NewCarbon(metricsPerSecond)

	suc6, resp := graphite.RetryGraphite8080("perSecond(metrictank.stats.docker-env.*.input.carbon.metricdata.received.counter32)", "-8s", 18, func(resp graphite.Response) bool {
		exp := []string{
			"perSecond(metrictank.stats.docker-env.default.input.carbon.metricdata.received.counter32)",
		}
		a := graphite.ValidateTargets(exp)(resp)
		b := graphite.ValidatorLenNulls(1, 8)(resp)
		c := graphite.ValidatorAvgWindowed(8, graphite.Ge(metricsPerSecond))(resp)
		log.Printf("condition target names %t - condition len & nulls %t - condition avg value %t", a, b, c)
		return a && b && c
	})
	if !suc6 {
		grafana.PostAnnotation("TestBaseIngestWorkload:FAIL")
		t.Fatalf("cluster did not reach a state where the MT instance processes at least %d points per second. last response was: %s", metricsPerSecond, spew.Sdump(resp))
	}
}
