package chaos

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"reflect"
	"testing"
	"time"

	"github.com/davecgh/go-spew/spew"
)

// TODO: cleanup when ctrl-C go test (teardown all containers)

var tracker *Tracker

func TestMain(m *testing.M) {
	ctx, cancelFunc := context.WithCancel(context.Background())

	fmt.Println("stopping docker-chaos stack should it be running...")
	cmd := exec.CommandContext(ctx, "docker-compose", "down")
	cmd.Dir = path("docker/docker-chaos")
	err := cmd.Start()
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println("launching docker-chaos stack...")
	cmd = exec.CommandContext(ctx, path("docker/launch.sh"), "docker-chaos")
	cmd.Env = append(cmd.Env, "MT_CLUSTER_MIN_AVAILABLE_SHARDS=12")

	tracker, err = NewTracker(cmd, false, false, "launch-stdout", "launch-stderr")
	if err != nil {
		log.Fatal(err)
	}

	err = cmd.Start()
	if err != nil {
		log.Fatal(err)
	}

	retcode := m.Run()

	fmt.Println("stopping docker-compose stack...")
	cancelFunc()
	if err := cmd.Wait(); err != nil {
		log.Printf("ERROR: could not cleanly shutdown running docker-compose command: %s", err)
		retcode = 1
	}

	os.Exit(retcode)
}

func TestClusterStartup(t *testing.T) {
	matchers := []Matcher{
		{Str: "metrictank0_1.*metricIndex initialized.*starting data consumption$"},
		{Str: "metrictank1_1.*metricIndex initialized.*starting data consumption$"},
		{Str: "metrictank2_1.*metricIndex initialized.*starting data consumption$"},
		{Str: "metrictank3_1.*metricIndex initialized.*starting data consumption$"},
		{Str: "metrictank4_1.*metricIndex initialized.*starting data consumption$"},
		{Str: "metrictank5_1.*metricIndex initialized.*starting data consumption$"},
		{Str: "grafana.*Initializing HTTP Server.*:3000"},
	}
	select {
	case <-tracker.Match(matchers):
	case <-time.After(time.Second * 40):
		postAnnotation("TestClusterStartup:FAIL")
		t.Fatal("timed out while waiting for all metrictank instances to come up")
	}
}

func TestClusterBaseIngestWorkload(t *testing.T) {
	postAnnotation("TestClusterBaseIngestWorkload:begin")

	go fakeMetrics()

	suc6, resp := retryGraphite("perSecond(metrictank.stats.docker-cluster.*.input.kafka-mdm.metrics_received.counter32)", "-8s", 18, func(resp response) bool {
		exp := []string{
			"perSecond(metrictank.stats.docker-cluster.metrictank0.input.kafka-mdm.metrics_received.counter32)",
			"perSecond(metrictank.stats.docker-cluster.metrictank1.input.kafka-mdm.metrics_received.counter32)",
			"perSecond(metrictank.stats.docker-cluster.metrictank2.input.kafka-mdm.metrics_received.counter32)",
			"perSecond(metrictank.stats.docker-cluster.metrictank3.input.kafka-mdm.metrics_received.counter32)",
			"perSecond(metrictank.stats.docker-cluster.metrictank4.input.kafka-mdm.metrics_received.counter32)",
			"perSecond(metrictank.stats.docker-cluster.metrictank5.input.kafka-mdm.metrics_received.counter32)",
		}
		// avg rate must be 4 (metrics ingested per second by each instance)
		return validateTargets(exp)(resp) && validatorAvgWindowed(8, 4)(resp)
	})
	if !suc6 {
		postAnnotation("TestClusterBaseIngestWorkload:FAIL")
		t.Fatalf("cluster did not reach a state where each MT instance receives 4 points per second. last response was: %s", spew.Sdump(resp))
	}

	suc6, resp = retryMT("sum(some.id.of.a.metric.*)", "-16s", 20, validateCorrect(12))
	if !suc6 {
		postAnnotation("TestClusterBaseIngestWorkload:FAIL")
		t.Fatalf("could not query correct result set. sum of 12 series, each valued 1, should result in 12.  last response was: %s", spew.Sdump(resp))
	}
}

func TestQueryWorkload(t *testing.T) {
	postAnnotation("TestQueryWorkload:begin")

	results := checkMT([]int{6060, 6061, 6062, 6063, 6064, 6065}, "sum(some.id.of.a.metric.*)", "-14s", time.Minute, 6000, validateCorrect(12))

	exp := checkResults{
		valid:   []int{6000},
		empty:   0,
		timeout: 0,
		other:   0,
	}
	if !reflect.DeepEqual(exp, results) {
		postAnnotation("TestQueryWorkload:FAIL")
		t.Fatalf("expected only correct results. got %s", spew.Sdump(results))
	}
}

// TestIsolateOneInstance tests what happens during the isolation of one instance, when min-available-shards is 12
// this should happen:
// at all times, all queries to all of the remaining nodes should be successful
// since they have at least 1 instance running for each shard.
// the isolated shard should either return correct replies, or errors (in two cases: when it marks any shards as down,
// but also before it does, but fails to get data via clustered requests from peers)
func TestIsolateOneInstance(t *testing.T) {
	postAnnotation("TestIsolateOneInstance:begin")
	numReqMt4 := 1200

	mt4ResultsChan := make(chan checkResults, 1)
	otherResultsChan := make(chan checkResults, 1)

	go func() {
		mt4ResultsChan <- checkMT([]int{6064}, "sum(some.id.of.a.metric.*)", "-15s", time.Minute, numReqMt4, validateCorrect(12), validateCode(503))
	}()
	go func() {
		otherResultsChan <- checkMT([]int{6060, 6061, 6062, 6063, 6065}, "sum(some.id.of.a.metric.*)", "-15s", time.Minute, 6000, validateCorrect(12))
	}()

	// now go ahead and isolate for 30s
	isolate([]string{"metrictank4"}, []string{"metrictank0", "metrictank1", "metrictank2", "metrictank3", "metrictank5"}, "30s")

	// collect results of the minute long experiment
	mt4Results := <-mt4ResultsChan
	otherResults := <-otherResultsChan

	// validate results of isolated node
	if mt4Results.valid[0]+mt4Results.valid[1] != numReqMt4 {
		t.Fatalf("expected mt4 to return either correct or erroring responses (total %d). got %s", numReqMt4, spew.Sdump(mt4Results))
	}
	if mt4Results.valid[1] < numReqMt4*30/100 {
		// the instance is completely down for 30s of the 60s experiment run, but we allow some slack
		t.Fatalf("expected at least 30%% of all mt4 results to succeed. did %d queries. got %s", numReqMt4, spew.Sdump(mt4Results))
	}

	// validate results of other cluster nodes
	exp := checkResults{
		valid:   []int{6000},
		empty:   0,
		timeout: 0,
		other:   0,
	}
	if !reflect.DeepEqual(exp, otherResults) {
		postAnnotation("TestIsolateOneInstance:FAIL")
		t.Fatalf("expected only correct results for all cluster nodes. got %s", spew.Sdump(otherResults))
	}
}

func TestHang(t *testing.T) {
	postAnnotation("TestHang:begin")
	t.Log("whatever happens, keep hanging for now, so that we can query grafana dashboards still")
	var ch chan struct{}
	<-ch
}

// maybe useful in the future, test also clean exit and rejoin like so:
//stop("metrictank4")
//time.AfterFunc(30*time.Second, func() {
//	start("metrictank4")
//})
