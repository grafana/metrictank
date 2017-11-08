package chaos

import (
	"context"
	"fmt"
	"log"
	"math"
	"os"
	"os/exec"
	"sync"
	"testing"
	"time"

	"github.com/grafana/metrictank/chaos/out/kafkamdm"
	"github.com/raintank/met/helper"
	"gopkg.in/raintank/schema.v1"
)

// TODO: cleanup when ctrl-C go test (teardomwnall containers)

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
	cmd := exec.CommandContext(ctx, "/home/dieter/go/src/github.com/grafana/metrictank/docker/launch.sh", "docker-chaos")

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

	suc6 := retryGraphite("perSecond(metrictank.stats.docker-cluster.*.input.kafka-mdm.metrics_received.counter32)", "-5s", 15, func(resp Response) bool {
		exp := []string{
			"perSecond(metrictank.stats.docker-cluster.metrictank0.input.kafka-mdm.metrics_received.counter32)",
			"perSecond(metrictank.stats.docker-cluster.metrictank1.input.kafka-mdm.metrics_received.counter32)",
			"perSecond(metrictank.stats.docker-cluster.metrictank2.input.kafka-mdm.metrics_received.counter32)",
			"perSecond(metrictank.stats.docker-cluster.metrictank3.input.kafka-mdm.metrics_received.counter32)",
			"perSecond(metrictank.stats.docker-cluster.metrictank4.input.kafka-mdm.metrics_received.counter32)",
			"perSecond(metrictank.stats.docker-cluster.metrictank5.input.kafka-mdm.metrics_received.counter32)",
		}
		if !validateTargets(resp, exp) {
			return false
		}
		for _, series := range resp {
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
			if sum/4 != 4 {
				return false
			}
		}
		return true
	})
	if !suc6 {
		t.Fatalf("cluster did not reach a state where each MT instance receives 4 points per second")
	}

	suc6 = retryMT("sum(some.id.of.a.metric.*)", "-5s", 10, func(resp Response) bool {
		if len(resp) != 1 {
			return false
		}
		points := resp[0].Datapoints
		// last point can sometimes be null
		for _, p := range points[:len(points)-1] {
			if math.IsNaN(p.Val) {
				return false
			}
			if p.Val != 12 {
				return false
			}
		}
		return true
	})
	if !suc6 {
		t.Fatalf("could not query correct result set. sum of 12 series, each valued 1, should result in 12")
	}
}

func TestIsolateOneInstance(t *testing.T) {
	t.Log("Starting TestIsolateOneInstance)")
	tracker.LogStdout(true)
	tracker.LogStderr(true)
	pre := time.Now()
	isolate("dockerchaos_metrictank4_1", "30s")
	tick := time.NewTicker(10 * time.Millisecond)
	wg := &sync.WaitGroup{}
	check := func(wg *sync.WaitGroup) {
		// only try this once. at this point, no request is allowed to fail. cluster should be 100% reliable
		resp := renderQuery("http://localhost:6060", "sum(some.id.of.a.metric.*)", "-5s")
		if len(resp) != 1 {
			return false
		}
		points := resp[0].Datapoints
		// last point can sometimes be null
		for _, p := range points[:len(points)-1] {
			if math.IsNaN(p.Val) {
				return false
			}
			if p.Val != 12 {
				return false
			}
		}
		return true
		if !suc6 {
			t.Fatalf("could not query correct result set. sum of 12 series, each valued 1, should result in 12")
		}
		wg.Done()
	}
	for t := range tick.C {
		if time.Since(pre) > 45*time.Second {
			tick.Stop()
			break
		}
		wg.Add(1)
		go check()
	}
	wg.Wait()
	if time.Since(pre) > 50*time.Second {
		t.Fatalf("had to wait too long for requests to complete. system doesn't perform well enough to make sure we get enough requests")
	}
}
func TestHang(t *testing.T) {
	t.Log("whatever happens, keep hanging for now, so that we can query grafana dashboards still")
	var ch chan struct{}
	<-ch
}
