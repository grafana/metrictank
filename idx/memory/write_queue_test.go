package memory

import (
	"fmt"
	"testing"
	"time"

	"github.com/raintank/schema"
)

func TestWriteQueue(t *testing.T) {
	_writeQueueEnabled := writeQueueEnabled
	defer func() {
		writeQueueEnabled = _writeQueueEnabled
	}()
	writeQueueEnabled = true
	ix := New()
	ix.Init()
	defer func() {
		ix.Stop()
		ix = nil
	}()

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

	nodes, err = ix.Find(1, "some.metric.*", 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != writeMaxBatchSize {
		t.Fatalf("expected %d nodes in the index. Instead found %d", writeMaxBatchSize, len(nodes))
	}
}
