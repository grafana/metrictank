package carbon

import (
	"fmt"
	"net"
	"regexp"
	"sync"
	"testing"
	"time"

	"github.com/benbjohnson/clock"
	"github.com/lomik/go-carbon/persister"
	"github.com/raintank/metrictank/cluster"
	"github.com/raintank/metrictank/idx/memory"
	"github.com/raintank/metrictank/input"
	"github.com/raintank/metrictank/mdata"
	"github.com/raintank/metrictank/mdata/cache"
	"github.com/raintank/metrictank/usage"
	"gopkg.in/raintank/schema.v1"
)

func Test_HandleMessage(t *testing.T) {
	cluster.Init("default", "test", time.Now(), "http", 6060)
	store := mdata.NewDevnullStore()
	aggmetrics := mdata.NewAggMetrics(store, &cache.MockCache{}, 600, 10, 800, 8000, 10000, 0, make([]mdata.AggSetting, 0))
	metricIndex := memory.New()
	metricIndex.Init()
	usage := usage.New(300, aggmetrics, metricIndex, clock.New())
	Enabled = true
	addr = "localhost:2003"
	var err error
	s := persister.Schema{
		Name:         "default",
		RetentionStr: "1s:1d",
		Pattern:      regexp.MustCompile(".*"),
	}
	s.Retentions, err = persister.ParseRetentionDefs(s.RetentionStr)
	if err != nil {
		panic(err)
	}

	schemas = persister.WhisperSchemas{s}
	c := New()
	// note: we could better create a mock handler that tracks Process calls
	// rather then having to rely on the real one and index.
	c.Start(input.NewDefaultHandler(aggmetrics, metricIndex, usage, "carbon"))

	allMetrics := make(map[string]int)
	var mu sync.Mutex
	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(i int, t *testing.T) {
			metrics := test_handleMessage(i, t)
			mu.Lock()
			for mId, id := range metrics {
				allMetrics[mId] = id
			}
			mu.Unlock()
			wg.Done()
		}(i, t)
	}
	wg.Wait()
	c.Stop()
	defs := metricIndex.List(-1)
	if len(defs) != len(allMetrics) {
		t.Fatalf("query for org -1 should result in %d distinct metrics. not %d", len(allMetrics), len(defs))
	}

	for _, d := range defs {
		id := allMetrics[d.Id]
		if d.Name != fmt.Sprintf("some.id.%d", id) {
			t.Fatalf("incorrect name for %s : %s, expected %s", d.Id, d.Name, fmt.Sprintf("some.id.%d", id))
		}
		if d.OrgId != 1 {
			t.Fatalf("incorrect OrgId for %s : %d", d.Id, d.OrgId)
		}
	}
}

func test_handleMessage(worker int, t *testing.T) map[string]int {
	conn, _ := net.Dial("tcp", "127.0.0.1:2003")
	defer conn.Close()
	metrics := make(map[string]int)
	for m := 0; m < 4; m++ {
		id := (worker + 1) * (m + 1)
		t.Logf("worker %d metric %d -> adding metric with id %d and orgid %d", worker, m, id, 1)
		md := &schema.MetricData{
			Name:     fmt.Sprintf("some.id.%d", id),
			Metric:   fmt.Sprintf("some.id.%d", id),
			Interval: 1,
			Value:    1234.567,
			Unit:     "unknown",
			Time:     int64(id),
			Mtype:    "gauge",
			Tags:     []string{},
			OrgId:    1, // admin org
		}
		md.SetId()
		metrics[md.Id] = id
		t.Logf("%s = %s", md.Id, md.Name)
		_, err := fmt.Fprintf(conn, fmt.Sprintf("%s %f %d\n", md.Name, md.Value, md.Time))
		if err != nil {
			t.Fatal(err)
		}
	}
	// as soon as this function ends, the server will close the socket.  We need to sleep here
	// to ensure that the packets have time to be procesed by the kernel and passed to the server.
	time.Sleep(time.Millisecond * 100)
	return metrics
}
