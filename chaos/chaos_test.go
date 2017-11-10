package chaos

import (
	"context"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"os/exec"
	"reflect"
	"testing"
	"time"

	"github.com/grafana/metrictank/chaos/out/kafkamdm"
	"github.com/raintank/met/helper"
	"gopkg.in/raintank/schema.v1"
)

// TODO: cleanup when ctrl-C go test (teardown all containers)

const numPartitions = 12

var tracker *Tracker
var metrics []*schema.MetricData

func init() {
	for i := 0; i < numPartitions; i++ {
		name := fmt.Sprintf("some.id.of.a.metric.%d", i)
		m := &schema.MetricData{
			OrgId:    1,
			Name:     name,
			Metric:   name,
			Interval: 1,
			Value:    1,
			Unit:     "s",
			Mtype:    "gauge",
		}
		m.SetId()
		metrics = append(metrics, m)
	}
}

func TestMain(m *testing.M) {
	ctx, cancelFunc := context.WithCancel(context.Background())
	cmd := exec.CommandContext(ctx, path("docker/launch.sh"), "docker-chaos")
	cmd.Env = append(cmd.Env, "MT_CLUSTER_MIN_AVAILABLE_SHARDS=12")

	var err error
	tracker, err = NewTracker(cmd, false, false)
	if err != nil {
		log.Fatal(err)
	}

	err = cmd.Start()
	if err != nil {
		log.Fatal(err)
	}

	retcode := m.Run()

	fmt.Println("stopping the docker-compose stack...")
	cancelFunc()
	if err := cmd.Wait(); err != nil {
		log.Printf("ERROR: could not cleanly shutdown running docker-compose command: %s", err)
		retcode = 1
	}

	os.Exit(retcode)
}

func TestClusterStartup(t *testing.T) {
	pre := time.Now()
	// wait until MT's are up and connected to kafka and cassandra
	matchers := []Matcher{
		{
			Str: "metrictank0_1.*metricIndex initialized.*starting data consumption$",
		},
		{
			Str: "metrictank1_1.*metricIndex initialized.*starting data consumption$",
		},
		{
			Str: "metrictank2_1.*metricIndex initialized.*starting data consumption$",
		},
		{
			Str: "metrictank3_1.*metricIndex initialized.*starting data consumption$",
		},
		{
			Str: "metrictank4_1.*metricIndex initialized.*starting data consumption$",
		},
		{
			Str: "metrictank5_1.*metricIndex initialized.*starting data consumption$",
		},
	}
	ch := tracker.Match(matchers)
	select {
	case <-ch:
		t.Logf("cluster started up in %s", time.Since(pre))
		return
	case <-time.After(time.Second * 40):
		t.Fatal("timed out while waiting for all metrictank instances to come up")
	}
}

// 1 metric to each of 12 partitions, each partition replicated twice = expect total workload across cluster of 24Hz
func TestClusterBaseWorkload(t *testing.T) {

	//	tracker.LogStdout(true)
	//	tracker.LogStderr(true)

	go func() {
		t.Log("Starting kafka publishing")
		stats, _ := helper.New(false, "", "standard", "", "")
		out, err := kafkamdm.New("mdm", []string{"localhost:9092"}, "none", stats, "lastNum")
		if err != nil {
			log.Fatal(4, "failed to create kafka-mdm output. %s", err)
		}
		ticker := time.NewTicker(time.Second)

		for tick := range ticker.C {
			unix := tick.Unix()
			for i := range metrics {
				metrics[i].Time = unix
			}
			err := out.Flush(metrics)
			if err != nil {
				t.Fatalf("failed to send data to kafka: %s", err)
			}
		}
	}()

	suc6 := retryGraphite("perSecond(metrictank.stats.docker-cluster.*.input.kafka-mdm.metrics_received.counter32)", "-5s", 15, func(resp response) bool {
		exp := []string{
			"perSecond(metrictank.stats.docker-cluster.metrictank0.input.kafka-mdm.metrics_received.counter32)",
			"perSecond(metrictank.stats.docker-cluster.metrictank1.input.kafka-mdm.metrics_received.counter32)",
			"perSecond(metrictank.stats.docker-cluster.metrictank2.input.kafka-mdm.metrics_received.counter32)",
			"perSecond(metrictank.stats.docker-cluster.metrictank3.input.kafka-mdm.metrics_received.counter32)",
			"perSecond(metrictank.stats.docker-cluster.metrictank4.input.kafka-mdm.metrics_received.counter32)",
			"perSecond(metrictank.stats.docker-cluster.metrictank5.input.kafka-mdm.metrics_received.counter32)",
		}
		if !validateTargets(exp)(resp) {
			return false
		}
		for _, series := range resp.r {
			var sum float64
			if len(series.Datapoints) != 5 {
				return false
			}
			// skip the first point. it always seems to be null for some reason
			for _, p := range series.Datapoints[1:] {
				if math.IsNaN(p.Val) {
					return false
				}
				sum += p.Val
			}
			// avg of all (4) datapoints must be 4 (metrics ingested per second by each instance)
			if sum/4 != 4 {
				return false
			}
		}
		return true
	})
	if !suc6 {
		t.Fatalf("cluster did not reach a state where each MT instance receives 4 points per second")
	}

	suc6 = retryMT("sum(some.id.of.a.metric.*)", "-5s", 10, validateCorrect(12))
	if !suc6 {
		t.Fatalf("could not query correct result set. sum of 12 series, each valued 1, should result in 12")
	}
}

// TestIsolateOneInstance tests what happens during the isolation of one instance, when min-available-shards is 12
// this should happen:
// at all times, all queries to all of the remaining nodes should be successful
// since they have at least 1 instance running for each shard.
// the isolated shard should either return correct replies, or errors (in two cases: when it marks any shards as down,
// but also before it does, but fails to get data via clustered requests from peers)
//. TODO: in production do we stop querying isolated peers?

func TestIsolateOneInstance(t *testing.T) {
	t.Log("Starting TestIsolateOneInstance)")
	tracker.LogStdout(true)
	tracker.LogStderr(true)
	pre := time.Now()
	rand.Seed(pre.Unix())

	mt4ResultsChan := make(chan checkResults, 1)
	otherResultsChan := make(chan checkResults, 1)

	go func() {
		mt4ResultsChan <- checkMT([]int{6064}, "some.id.of.a.*", "-10s", time.Minute, 6000, validateCorrect(12), validateError)
	}()
	go func() {
		otherResultsChan <- checkMT([]int{6060, 6061, 6062, 6063, 6065}, "some.id.of.a.*", "-10s", time.Minute, 6000, validateCorrect(12))
	}()

	// now go ahead and isolate for 30s
	isolate("dockerchaos_metrictank4_1", "30s")

	// collect results of the minute long experiment
	mt4Results := <-mt4ResultsChan
	otherResults := <-otherResultsChan

	// validate results of isolated node
	if mt4Results.valid[0]+mt4Results.valid[1] != 6000 {
		t.Fatalf("expected mt4 to return either correct or erroring responses. got %v", mt4Results)
	}
	if mt4Results.valid[1] < 30*6000/100 {
		// the instance is completely down for 30s of the 60s experiment run, but we allow some slack
		t.Fatalf("expected at least 30%% of all mt4 results to succeed. got %v", mt4Results)
	}

	if mt4Results.timeout != 0 {
		t.Fatalf("expected mt4 to not timeout. got %v", mt4Results)
	}
	if mt4Results.invalid != 0 {
		t.Fatalf("expected mt4 to not invalid. got %v", mt4Results)
	}

	// validate results of other cluster nodes
	exp := checkResults{
		valid:   []int{6000},
		invalid: 0,
		timeout: 0,
	}
	if !reflect.DeepEqual(exp, otherResults) {
		t.Fatalf("expected only correct results for all cluster nodes. got %v", otherResults)
	}
}

func TestHang(t *testing.T) {
	t.Log("whatever happens, keep hanging for now, so that we can query grafana dashboards still")
	var ch chan struct{}
	<-ch
}
