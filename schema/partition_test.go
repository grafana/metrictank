package schema

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
)

// getSeriesNames returns a count-length slice of random strings comprised of the prefix and count nodes.like.this
func getSeriesNames(depth, count int, prefix string) []string {
	series := make([]string, count)
	for i := 0; i < count; i++ {
		ns := make([]string, depth)
		for j := 0; j < depth; j++ {
			ns[j] = getRandomString(4)
		}
		series[i] = prefix + "." + strings.Join(ns, ".")
	}
	return series
}

// source: https://github.com/gogits/gogs/blob/9ee80e3e5426821f03a4e99fad34418f5c736413/modules/base/tool.go#L58
func getRandomString(n int, alphabets ...byte) string {
	const alphanum = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	var bytes = make([]byte, n)
	rand.Read(bytes)
	for i, b := range bytes {
		if len(alphabets) == 0 {
			bytes[i] = alphanum[b%byte(len(alphanum))]
		} else {
			bytes[i] = alphabets[b%byte(len(alphabets))]
		}
	}
	return string(bytes)
}

// getMetricData returns a count-length slice of MetricData's with random Name and the given org id
func getMetricData(orgId uint32, depth, count, interval int, prefix string, tagged bool) []*MetricData {
	data := make([]*MetricData, count)
	series := getSeriesNames(depth, count, prefix)

	for i, s := range series {
		data[i] = &MetricData{
			Name:     s,
			OrgId:    int(orgId),
			Interval: interval,
		}
		if tagged {
			data[i].Tags = []string{fmt.Sprintf("series_id=%d", i)}
		}
		data[i].SetId()
	}

	return data
}

// getMetricDataWithCustomTags returns a count-length slice of MetricData's with random Name and the given orgId
// unique is a float between 0.0 and 1.0. A total of 10 tags will be added to each MetricData and unique determines how
// many will be identical vs unique. For example, a unique value of 0.4 will add 4 unique tags and 6 identical tags.
func getMetricDataWithCustomTags(orgId uint32, depth, count, interval int, prefix string, unique float32) []*MetricData {
	if unique < 0.0 || unique > 1.0 {
		panic("getMetricDataWithCustomTags: unique must be a value between 0.0 and 1.0")
	}

	tags := []string{
		"secondkey=anothervalue",
		"thirdkey=onemorevalue",
		"region=west",
		"os=ubuntu",
		"anothertag=somelongervalue",
		"manymoreother=lotsoftagstointern",
		"afewmoretags=forgoodmeasure",
		"onetwothreefourfivesix=seveneightnineten",
		"lotsandlotsoftags=morefunforeveryone",
		"goodforpeoplewhojustusetags=forbasicallyeverything",
	}

	uniqueNumber := 0

	data := make([]*MetricData, count)
	series := getSeriesNames(depth, count, prefix)

	for i, s := range series {
		data[i] = &MetricData{
			Name:     s,
			OrgId:    int(orgId),
			Interval: interval,
		}
		data[i].Tags = make([]string, 10)
		var j int
		for j = 0; j < int(unique*10); j++ {
			data[i].Tags[j] = fmt.Sprintf("unique_series_id%d=%d", uniqueNumber, uniqueNumber)
			uniqueNumber++
		}
		for j < 10 {
			data[i].Tags[j] = tags[j]
			j++
		}
		data[i].SetId()
	}

	return data
}

func TestPartitionWithOrg(t *testing.T) {
	org := uint32(10)
	series := getMetricData(org, 2, 100, 10, "metric.org1", false)
	last := int32(0)
	for i, md := range series {
		p, err := md.PartitionID(PartitionByOrg, int32(32))
		if err != nil {
			t.Fatalf("failed to get partition on %s with orgId=%d", md.Id, md.OrgId)
		}
		if i > 0 {
			if p != last {
				t.Fatalf("partition expected to be same as last. last=%d, p=%d", last, p)
			}
		}
		last = p
	}
}

func TestPartitionWithSeries(t *testing.T) {
	partitionCount := int32(32)
	metricCount := 5000
	series := getMetricData(1, 2, metricCount, 10, "metric.org1", false)

	partitions := make(map[int32]int)
	for _, md := range series {
		p, err := md.PartitionID(PartitionBySeries, partitionCount)
		if err != nil {
			t.Fatalf("failed to get partition on %s with orgId=%d", md.Id, md.OrgId)
		}
		if p < 0 {
			t.Fatalf("partition expected to a positive number, p=%d", p)
		}

		partitions[p] = partitions[p] + 1
	}
	if int32(len(partitions)) < partitionCount {
		t.Fatalf("with %d series only %d/%d partitions seen", metricCount, len(partitions), partitionCount)
	}
}

func TestPartitionWithSeriesWithTags(t *testing.T) {
	partitionCount := int32(32)
	metricCount := 5000
	series := getMetricData(1, 2, metricCount, 10, "metric.org1", false)

	partitions := make(map[int32]int)
	for _, md := range series {
		p, err := md.PartitionID(PartitionBySeriesWithTags, partitionCount)
		if err != nil {
			t.Fatalf("failed to get partition on %s with orgId=%d", md.Id, md.OrgId)
		}
		if p < 0 {
			t.Fatalf("partition expected to a positive number, p=%d", p)
		}

		partitions[p] = partitions[p] + 1
	}
	if int32(len(partitions)) < partitionCount {
		t.Fatalf("with %d series only %d/%d partitions seen", metricCount, len(partitions), partitionCount)
	}
}

func TestPartitionWithSeriesWithTagsFnv(t *testing.T) {
	partitionCount := int32(32)
	metricCount := 5000
	series := getMetricData(1, 2, metricCount, 10, "metric.org1", false)

	partitions := make(map[int32]int)
	for _, md := range series {
		p, err := md.PartitionID(PartitionBySeriesWithTagsFnv, partitionCount)
		if err != nil {
			t.Fatalf("failed to get partition on %s with orgId=%d", md.Id, md.OrgId)
		}
		if p < 0 {
			t.Fatalf("partition expected to a positive number, p=%d", p)
		}

		pBySeries, err := md.PartitionID(PartitionBySeries, partitionCount)
		if err != nil {
			t.Fatalf("failed to get partition on %s with orgId=%d", md.Id, md.OrgId)
		}
		if p != pBySeries {
			t.Fatalf("partitionBySeriesWithTagsFnv(%d) and partitionBySeries(%d) should yield the same partition ID.", p, pBySeries)
		}

		partitions[p] = partitions[p] + 1
	}
	if int32(len(partitions)) < partitionCount {
		t.Fatalf("with %d series only %d/%d partitions seen", metricCount, len(partitions), partitionCount)
	}
}

func benchPartitioning(method PartitionByMethod, b *testing.B) {
	partitionCount := int32(32)
	metricCount := 5000
	series := getMetricDataWithCustomTags(1, 2, metricCount, 10, "m", 0.2)
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		p, err := series[i%metricCount].PartitionID(method, partitionCount)
		if err != nil {
			b.Fatalf("failed to get partition of series. err=%s", err)
		}
		if p < 0 {
			b.Fatalf("invalid partition ID returned for series. got %d", p)
		}
	}
}

func BenchmarkPartitionByOrg(b *testing.B) {
	benchPartitioning(PartitionByOrg, b)
}

func BenchmarkPartitionBySeries(b *testing.B) {
	benchPartitioning(PartitionBySeries, b)
}

func BenchmarkPartitionBySeriesWithTags(b *testing.B) {
	benchPartitioning(PartitionBySeriesWithTags, b)
}

func BenchmarkPartitionBySeriesWithTagsFnv(b *testing.B) {
	benchPartitioning(PartitionBySeriesWithTagsFnv, b)
}
