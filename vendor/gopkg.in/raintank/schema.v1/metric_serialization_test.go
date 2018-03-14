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
