package schema

import (
	"math/rand"
	"strconv"
	"testing"
)

func checkErr(tb testing.TB, err error) {
	if err != nil {
		tb.Fatalf("error: %s", err.Error())
	}
}

func getDifferentMetricDataArray(amount int) []*MetricData {
	names := []string{
		"litmus.http.error_state.",
		"litmus.hello.dieter_plaetinck.be",
		"litmus.ok.raintank_dns_error_state_foo_longer",
		"hi.alerting.state",
	}
	intervals := []int{1, 10, 60}
	tags := [][]string{
		{
			"foo:bar",
			"endpoint_id:25",
			"collector_id:hi",
		},
		{
			"foo_bar:quux",
			"endpoint_id:25",
			"collector_id:hi",
			"some_other_tag:ok",
		},
	}
	baseTime := int64(1512345678) // somewhat randomly chosen but realistic (dec 2017)
	r := rand.New(rand.NewSource(438))
	out := make([]*MetricData, amount)
	for i := 0; i < amount; i++ {
		out[i] = &MetricData{
			OrgId:    i,
			Name:     names[i%len(names)] + "foo.bar" + strconv.Itoa(i),
			Metric:   names[i%len(names)],
			Interval: intervals[i%len(intervals)],
			Value:    r.Float64(),
			Unit:     "foo",
			Time:     baseTime + int64(i),
			Mtype:    "bleh",
			Tags:     tags[i%len(tags)],
		}
		out[i].SetId()
	}
	return out
}

func getDifferentMetricPoints(amount int) []MetricPoint {
	ids := []string{
		"1.6f7f966befca84c637b9e1800f5fc9de",
		"1322.03b1159fab0bc475dd3d94dcde4bf5fa",
		"65298.047a7232d9ab39e614927d6f8c984f3a",
		"598919.abdd07523ee7a8b1afc34b452edfcec9",
	}
	baseTime := uint32(1512345678) // somewhat randomly chosen but realistic (dec 2017)
	r := rand.New(rand.NewSource(438))
	out := make([]MetricPoint, amount)
	for i := 0; i < amount; i++ {
		out[i] = MetricPoint{
			Id:    ids[i%len(ids)],
			Value: r.Float64(),
			Time:  baseTime + uint32(i),
		}
	}
	return out
}
