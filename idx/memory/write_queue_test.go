package memory

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/grafana/metrictank/schema"
)

func TestWriteQueue(t *testing.T) {
	_writeQueueEnabled := writeQueueEnabled
	defer func() {
		writeQueueEnabled = _writeQueueEnabled
	}()
	writeQueueEnabled = true
	ix := New()
	ix.Init()
	defer ix.Stop()

	count := writeMaxBatchSize - 1
	for i := 0; i < count; i++ {
		name := fmt.Sprintf("some.metric.%d", i)
		data := &schema.MetricData{
			Name:     name,
			OrgId:    1,
			Interval: 1,
			Tags:     []string{},
			Time:     time.Now().Unix(),
		}
		data.SetId()
		mkey, _ := schema.MKeyFromString(data.Id)
		ix.AddOrUpdate(mkey, data, getPartition(data))
	}

	nodes, err := ix.Find(1, "some.metric.*", 0)
	if err != nil {
		t.Fatal(err)
	}

	// we should get no results, as the archives should all be queued and not written to the index yet.
	if len(nodes) != 0 {
		t.Fatalf("expected 0 nodes in the index. Instead found %d", len(nodes))
	}

	// add 1 more series to the index, which will trigger the writeQueue to flush
	data := &schema.MetricData{
		Name:     "some.metric.foo",
		OrgId:    1,
		Interval: 1,
		Tags:     []string{},
		Time:     time.Now().Unix(),
	}
	data.SetId()
	mkey, _ := schema.MKeyFromString(data.Id)
	ix.AddOrUpdate(mkey, data, getPartition(data))

	// TODO - make this less flaky (add flush counter? Flush wait func?)
	time.Sleep(100 * time.Millisecond)

	nodes, err = ix.Find(1, "some.metric.*", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != writeMaxBatchSize {
		t.Fatalf("expected %d nodes in the index. Instead found %d", writeMaxBatchSize, len(nodes))
	}
}

func TestWriteQueueMultiThreads(t *testing.T) {
	_writeQueueEnabled := writeQueueEnabled
	defer func() {
		writeQueueEnabled = _writeQueueEnabled
	}()
	writeQueueEnabled = true
	ix := New()
	ix.Init()
	defer ix.Stop()

	var wg sync.WaitGroup
	numThreads := 4
	for t := 0; t < numThreads; t++ {
		wg.Add(1)
		go func(threadNum int) {
			count := writeMaxBatchSize - 1
			for i := 0; i < count; i++ {
				name := fmt.Sprintf("some.metric.%d.%d", threadNum, i)
				data := &schema.MetricData{
					Name:     name,
					OrgId:    1,
					Interval: 1,
					Tags:     []string{},
					Time:     time.Now().Unix(),
				}
				data.SetId()
				mkey, _ := schema.MKeyFromString(data.Id)
				ix.AddOrUpdate(mkey, data, getPartition(data))
			}
			wg.Done()
		}(t)
	}

	wg.Wait()

	time.Sleep(10 * time.Millisecond)

	nodes, err := ix.Find(1, "some.metric.*.*", 0)
	if err != nil {
		t.Fatal(err)
	}

	minNodes := writeMaxBatchSize * (numThreads - 1)
	maxNodes := (writeMaxBatchSize - 1) * numThreads
	if len(nodes) < minNodes {
		t.Fatalf("expected at least %d nodes in the index. Instead found %d", minNodes, len(nodes))
	} else if len(nodes) > maxNodes {
		t.Fatalf("expected at most %d nodes in the index. Instead found %d", maxNodes, len(nodes))
	}
}
