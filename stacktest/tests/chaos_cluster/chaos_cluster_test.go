package chaos_cluster

import (
	"flag"
	"os"
	"os/exec"
	"syscall"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/google/go-cmp/cmp"
	"github.com/grafana/metrictank/logger"
	"github.com/grafana/metrictank/stacktest/docker"
	"github.com/grafana/metrictank/stacktest/fakemetrics"
	"github.com/grafana/metrictank/stacktest/grafana"
	"github.com/grafana/metrictank/stacktest/graphite"
	"github.com/grafana/metrictank/stacktest/track"
	"github.com/grafana/metrictank/test"
	log "github.com/sirupsen/logrus"
)

// TODO: cleanup when ctrl-C go test (teardown all containers)

const numPartitions = 12

var tracker *track.Tracker
var fm *fakemetrics.FakeMetrics

func init() {
	formatter := &logger.TextFormatter{}
	formatter.TimestampFormat = "2006-01-02 15:04:05.000"
	log.SetFormatter(formatter)
	log.SetLevel(log.InfoLevel)
}
func TestMain(m *testing.M) {
	flag.Parse()
	if testing.Short() {
		log.Println("skipping chaos cluster test in short mode")
		return
	}

	log.Println("stopping docker-chaos stack should it be running...")
	cmd := exec.Command("docker-compose", "down")
	cmd.Dir = test.Path("docker/docker-chaos")
	err := cmd.Start()
	if err != nil {
		log.Fatal(err.Error())
	}
	err = cmd.Wait()
	if err != nil {
		log.Fatal(err.Error())
	}

	version := exec.Command("docker-compose", "version")
	output, err := version.CombinedOutput()
	if err != nil {
		log.Fatal(err.Error())
	}
	log.Println(string(output))

	log.Println("launching docker-chaos stack...")
	// TODO: should probably use -V flag here.
	// introduced here https://github.com/docker/compose/releases/tag/1.19.0
	// but circleCI machine image still stuck with 1.14.0
	cmd = exec.Command("docker-compose", "up", "--force-recreate")
	cmd.Dir = test.Path("docker/docker-chaos")
	// note we rely on this technique to pass this setting on to all MT's
	// https://docs.docker.com/compose/environment-variables/#pass-environment-variables-to-containers
	cmd.Env = append(cmd.Env, "MT_CLUSTER_MIN_AVAILABLE_SHARDS=12")

	tracker, err = track.NewTracker(cmd, true, true, "launch-stdout", "launch-stderr")
	if err != nil {
		log.Fatal(err.Error())
	}
	err = cmd.Start()
	if err != nil {
		log.Fatal(err.Error())
	}

	// note: if docker-compose didn't start properly - e.g. it bails out due to unsupported config -
	// what typically happens is the tests will fail and we will exit, and not see the docker-compose problem
	// until further manual inspection or reproducing.
	// in a future version, we should add more rigorous checks to make sure dockor-compose didn't exit >0 and that the containers started properly
	// we may want to use 'docker-compose up -d' for that
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

func TestClusterStartup(t *testing.T) {
	matchers := []track.Matcher{
		{Str: "metrictank0.1.*metricIndex initialized.*starting data consumption$"},
		{Str: "metrictank1.1.*metricIndex initialized.*starting data consumption$"},
		{Str: "metrictank2.1.*metricIndex initialized.*starting data consumption$"},
		{Str: "metrictank3.1.*metricIndex initialized.*starting data consumption$"},
		{Str: "metrictank4.1.*metricIndex initialized.*starting data consumption$"},
		{Str: "metrictank5.1.*metricIndex initialized.*starting data consumption$"},
		{Str: "grafana.*HTTP Server Listen.*3000"},
		{Str: "zookeeper entered RUNNING state"},
		{Str: "kafka entered RUNNING state"},
	}
	log.Println("Matchers: ", matchers)
	select {
	case <-tracker.Match(matchers, true):
		log.Println("stack now running.")
		log.Println("Go to http://localhost:3000 (and login as admin:admin) to see what's going on")
	case <-time.After(time.Second * 80):
		grafana.PostAnnotation("TestClusterStartup:FAIL")
		t.Fatal("timed out while waiting for all metrictank instances to come up")
	}
}

func TestClusterBaseIngestWorkload(t *testing.T) {
	grafana.PostAnnotation("TestClusterBaseIngestWorkload:begin")

	// generate exactly numPartitions metrics, numbered 0..numPartitions where each metric goes to the partition of the same number
	// each partition is consumed by 2 instances, and each instance consumes 4 partitions thus 4 metrics/s on average.
	fm = fakemetrics.NewKafka(numPartitions, 5*time.Second, false)

	req := graphite.RequestForLocalTestingGraphite("perSecond(metrictank.stats.docker-cluster.*.input.kafka-mdm.metricdata.received.counter32)", "-8s")

	exp := []string{
		"perSecond(metrictank.stats.docker-cluster.metrictank0.input.kafka-mdm.metricdata.received.counter32)",
		"perSecond(metrictank.stats.docker-cluster.metrictank1.input.kafka-mdm.metricdata.received.counter32)",
		"perSecond(metrictank.stats.docker-cluster.metrictank2.input.kafka-mdm.metricdata.received.counter32)",
		"perSecond(metrictank.stats.docker-cluster.metrictank3.input.kafka-mdm.metricdata.received.counter32)",
		"perSecond(metrictank.stats.docker-cluster.metrictank4.input.kafka-mdm.metricdata.received.counter32)",
		"perSecond(metrictank.stats.docker-cluster.metrictank5.input.kafka-mdm.metricdata.received.counter32)",
	}
	resp, ok := graphite.Retry(req, 18, graphite.ValidatorAnd(graphite.ValidateTargets(exp), graphite.ValidatorAvgWindowed(8, graphite.Eq(4))))
	if !ok {
		grafana.PostAnnotation("TestClusterBaseIngestWorkload:FAIL")
		t.Fatalf("cluster did not reach a state where each MT instance receives 4 points per second. last response was: %s", spew.Sdump(resp))
	}

	req = graphite.RequestForLocalTestingMT("sum(some.id.of.a.metric.*)", "-16s")
	resp, ok = graphite.Retry(req, 20, graphite.ValidateCorrect(12))
	if !ok {
		grafana.PostAnnotation("TestClusterBaseIngestWorkload:FAIL")
		t.Fatalf("could not query correct result set. sum of 12 series, each valued 1, should result in 12.  last response was: %s", spew.Sdump(resp))
	}
}

func TestQueryWorkload(t *testing.T) {
	grafana.PostAnnotation("TestQueryWorkload:begin")
	validators := []graphite.Validator{graphite.ValidateCorrect(12)}

	got := graphite.CheckMT([]int{6060, 6061, 6062, 6063, 6064, 6065}, "sum(some.id.of.a.metric.*)", "-14s", time.Minute, 600, validators...)
	exp := graphite.CheckResults{
		Validators: validators,
		Valid:      []int{600},
		Empty:      0,
		Timeout:    0,
		Other:      0,
	}
	if diff := cmp.Diff(exp, got); diff != "" {
		grafana.PostAnnotation("TestQueryWorkload:FAIL")
		t.Fatalf("expected only correct results. (-want +got):\n%s", diff)
	}
}

// TestIsolateOneInstance tests what happens during the isolation of one instance, when min-available-shards is 12
// this should happen:
// at all times, all queries to all of the remaining nodes should be successful
// since they have at least 1 instance running for each shard.
// the isolated shard should either return correct replies, or errors (in two cases: when it marks any shards as down,
// but also before it does, but fails to get data via clustered requests from peers)
func TestIsolateOneInstance(t *testing.T) {
	grafana.PostAnnotation("TestIsolateOneInstance:begin")
	numReqMt4 := 120
	validatorsOther := []graphite.Validator{graphite.ValidateCorrect(12)}
	mt4ResultsChan := make(chan graphite.CheckResults, 1)
	otherResultsChan := make(chan graphite.CheckResults, 1)

	go func() {
		mt4ResultsChan <- graphite.CheckMT([]int{6064}, "sum(some.id.of.a.metric.*)", "-15s", time.Minute, numReqMt4, graphite.ValidateCorrect(12), graphite.ValidateCode(503))
	}()
	go func() {
		otherResultsChan <- graphite.CheckMT([]int{6060, 6061, 6062, 6063, 6065}, "sum(some.id.of.a.metric.*)", "-15s", time.Minute, 600, validatorsOther...)
	}()

	// now go ahead and isolate for 30s
	docker.Isolate([]string{"metrictank4"}, []string{"metrictank0", "metrictank1", "metrictank2", "metrictank3", "metrictank5"}, "30s")

	// collect results of the minute long experiment
	mt4Results := <-mt4ResultsChan
	otherResults := <-otherResultsChan

	// validate results of isolated node
	if mt4Results.Valid[0]+mt4Results.Valid[1] != numReqMt4 {
		t.Fatalf("expected mt4 to return either correct or erroring responses (total %d). got %s", numReqMt4, spew.Sdump(mt4Results))
	}
	if mt4Results.Valid[0] < numReqMt4*30/100 {
		// the instance is completely down for 30s of the 60s experiment run, but we allow some slack
		t.Fatalf("expected at least 30%% of all mt4 results to succeed. did %d queries. got %s", numReqMt4, spew.Sdump(mt4Results))
	}

	// validate results of other cluster nodes
	exp := graphite.CheckResults{
		Validators: validatorsOther,
		Valid:      []int{600},
		Empty:      0,
		Timeout:    0,
		Other:      0,
	}
	if diff := cmp.Diff(exp, otherResults); diff != "" {
		grafana.PostAnnotation("TestIsolateOneInstance:FAIL")
		t.Fatalf("expected only correct results for all cluster nodes. (-want +got):\n%s", diff)
	}
}

//func TestHang(t *testing.T) {
//	grafana.PostAnnotation("TestHang:begin")
//	t.Log("whatever happens, keep hanging for now, so that we can query grafana dashboards still")
//	var ch chan struct{}
//	<-ch
//}

// maybe useful in the future, test also clean exit and rejoin like so:
//stop("metrictank4")
//time.AfterFunc(30*time.Second, func() {
//	start("metrictank4")
//})
