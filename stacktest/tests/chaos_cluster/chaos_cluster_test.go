package chaos_cluster

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"reflect"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"

	"github.com/davecgh/go-spew/spew"
	"github.com/grafana/metrictank/stacktest/docker"
	"github.com/grafana/metrictank/stacktest/fakemetrics"
	"github.com/grafana/metrictank/stacktest/grafana"
	"github.com/grafana/metrictank/stacktest/graphite"
	"github.com/grafana/metrictank/stacktest/track"
)

// TODO: cleanup when ctrl-C go test (teardown all containers)

const numPartitions = 12

var tracker *track.Tracker
var fm *fakemetrics.FakeMetrics

func TestMain(m *testing.M) {
	ctx, cancelFunc := context.WithCancel(context.Background())

	fmt.Println("stopping docker-chaos stack should it be running...")
	cmd := exec.CommandContext(ctx, "docker-compose", "down")
	cmd.Dir = docker.Path("docker/docker-chaos")
	err := cmd.Start()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Fatal("docker-compose down failed")
	}

	fmt.Println("launching docker-chaos stack...")
	cmd = exec.CommandContext(ctx, "docker-compose", "up", "--force-recreate", "-V")
	cmd.Env = append(cmd.Env, "MT_CLUSTER_MIN_AVAILABLE_SHARDS=12")

	tracker, err = track.NewTracker(cmd, false, false, "launch-stdout", "launch-stderr")
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Fatal("failed to create new tracker")
	}

	err = cmd.Start()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Fatal("docker-compose up failed")
	}

	retcode := m.Run()
	fm.Close()

	fmt.Println("stopping docker-compose stack...")
	cancelFunc()
	if err := cmd.Wait(); err != nil {
		log.WithFields(log.Fields{
			"error": err.Error(),
		}).Info("could not cleanly shutdown running docker-compose command")
		retcode = 1
	}

	os.Exit(retcode)
}

func TestClusterStartup(t *testing.T) {
	matchers := []track.Matcher{
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
		fmt.Println("stack now running.")
		fmt.Println("Go to http://localhost:3000 (and login as admin:admin) to see what's going on")
	case <-time.After(time.Second * 40):
		grafana.PostAnnotation("TestClusterStartup:FAIL")
		t.Fatal("timed out while waiting for all metrictank instances to come up")
	}
}

func TestClusterBaseIngestWorkload(t *testing.T) {
	grafana.PostAnnotation("TestClusterBaseIngestWorkload:begin")

	fm = fakemetrics.NewKafka(numPartitions)

	suc6, resp := graphite.RetryGraphite("perSecond(metrictank.stats.docker-cluster.*.input.kafka-mdm.metrics_received.counter32)", "-8s", 18, func(resp graphite.Response) bool {
		exp := []string{
			"perSecond(metrictank.stats.docker-cluster.metrictank0.input.kafka-mdm.metrics_received.counter32)",
			"perSecond(metrictank.stats.docker-cluster.metrictank1.input.kafka-mdm.metrics_received.counter32)",
			"perSecond(metrictank.stats.docker-cluster.metrictank2.input.kafka-mdm.metrics_received.counter32)",
			"perSecond(metrictank.stats.docker-cluster.metrictank3.input.kafka-mdm.metrics_received.counter32)",
			"perSecond(metrictank.stats.docker-cluster.metrictank4.input.kafka-mdm.metrics_received.counter32)",
			"perSecond(metrictank.stats.docker-cluster.metrictank5.input.kafka-mdm.metrics_received.counter32)",
		}
		// avg rate must be 4 (metrics ingested per second by each instance)
		return graphite.ValidateTargets(exp)(resp) && graphite.ValidatorAvgWindowed(8, graphite.Eq(4))(resp)
	})
	if !suc6 {
		grafana.PostAnnotation("TestClusterBaseIngestWorkload:FAIL")
		t.Fatalf("cluster did not reach a state where each MT instance receives 4 points per second. last response was: %s", spew.Sdump(resp))
	}

	suc6, resp = graphite.RetryMT("sum(some.id.of.a.metric.*)", "-16s", 20, graphite.ValidateCorrect(12))
	if !suc6 {
		grafana.PostAnnotation("TestClusterBaseIngestWorkload:FAIL")
		t.Fatalf("could not query correct result set. sum of 12 series, each valued 1, should result in 12.  last response was: %s", spew.Sdump(resp))
	}
}

func TestQueryWorkload(t *testing.T) {
	grafana.PostAnnotation("TestQueryWorkload:begin")

	results := graphite.CheckMT([]int{6060, 6061, 6062, 6063, 6064, 6065}, "sum(some.id.of.a.metric.*)", "-14s", time.Minute, 6000, graphite.ValidateCorrect(12))

	exp := graphite.CheckResults{
		Valid:   []int{6000},
		Empty:   0,
		Timeout: 0,
		Other:   0,
	}
	if !reflect.DeepEqual(exp, results) {
		grafana.PostAnnotation("TestQueryWorkload:FAIL")
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
	grafana.PostAnnotation("TestIsolateOneInstance:begin")
	numReqMt4 := 1200

	mt4ResultsChan := make(chan graphite.CheckResults, 1)
	otherResultsChan := make(chan graphite.CheckResults, 1)

	go func() {
		mt4ResultsChan <- graphite.CheckMT([]int{6064}, "sum(some.id.of.a.metric.*)", "-15s", time.Minute, numReqMt4, graphite.ValidateCorrect(12), graphite.ValidateCode(503))
	}()
	go func() {
		otherResultsChan <- graphite.CheckMT([]int{6060, 6061, 6062, 6063, 6065}, "sum(some.id.of.a.metric.*)", "-15s", time.Minute, 6000, graphite.ValidateCorrect(12))
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
	if mt4Results.Valid[1] < numReqMt4*30/100 {
		// the instance is completely down for 30s of the 60s experiment run, but we allow some slack
		t.Fatalf("expected at least 30%% of all mt4 results to succeed. did %d queries. got %s", numReqMt4, spew.Sdump(mt4Results))
	}

	// validate results of other cluster nodes
	exp := graphite.CheckResults{
		Valid:   []int{6000},
		Empty:   0,
		Timeout: 0,
		Other:   0,
	}
	if !reflect.DeepEqual(exp, otherResults) {
		grafana.PostAnnotation("TestIsolateOneInstance:FAIL")
		t.Fatalf("expected only correct results for all cluster nodes. got %s", spew.Sdump(otherResults))
	}
}

func TestHang(t *testing.T) {
	grafana.PostAnnotation("TestHang:begin")
	t.Log("whatever happens, keep hanging for now, so that we can query grafana dashboards still")
	var ch chan struct{}
	<-ch
}

// maybe useful in the future, test also clean exit and rejoin like so:
//stop("metrictank4")
//time.AfterFunc(30*time.Second, func() {
//	start("metrictank4")
//})
