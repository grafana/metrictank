package schema

import (
	"crypto/md5"
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
	ids := [][16]byte{
		md5.Sum([]byte("litmus.http.error_state.")),
		md5.Sum([]byte("litmus.hello.dieter_plaetinck.be")),
		md5.Sum([]byte("litmus.ok.raintank_dns_error_state_foo_longer")),
		md5.Sum([]byte("hi.alerting.state")),
	}
	baseTime := uint32(1512345678) // somewhat randomly chosen but realistic (dec 2017)
	r := rand.New(rand.NewSource(438))
	out := make([]MetricPoint, amount)
	for i := 0; i < amount; i++ {
		out[i] = MetricPoint{
			MKey: MKey{
				Key: ids[i%len(ids)],
				Org: uint32(i),
			},
			Value: r.Float64(),
			Time:  baseTime + uint32(i),
		}
	}
	return out
}
