package schema

import (
	"bytes"
	"encoding/gob"
	"encoding/json"
	"math/rand"
	"strconv"
	"testing"
)

func getDifferentMetrics(amount int) []*MetricData {
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
			Time:     r.Int63(),
			Mtype:    "bleh",
			Tags:     tags[i%len(tags)],
		}
	}
	return out
}

func BenchmarkSerialize3000MetricsJson(b *testing.B) {
	metrics := getDifferentMetrics(3000)
	b.ResetTimer()
	var size int
	for n := 0; n < b.N; n++ {
		i, err := json.Marshal(metrics)
		if err != nil {
			panic(err)
		}
		size = len(i)
	}
	b.Log("final size:", size)
}

func BenchmarkDeSerialize3000MetricsJson(b *testing.B) {
	metrics := getDifferentMetrics(3000)
	data, err := json.Marshal(metrics)
	if err != nil {
		panic(err)
	}
	out := make([]*MetricData, 0)
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		err := json.Unmarshal(data, &out)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkSerialize3000MetricsGob(b *testing.B) {
	metrics := getDifferentMetrics(3000)
	var size int
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		var buf bytes.Buffer
		enc := gob.NewEncoder(&buf)
		err := enc.Encode(metrics)
		if err != nil {
			panic(err)
		}
		size = buf.Len()
	}
	b.Log("final size:", size)
}
func BenchmarkDeSerialize3000MetricsGob(b *testing.B) {
	metrics := getDifferentMetrics(3000)
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	err := enc.Encode(metrics)
	if err != nil {
	}
	out := make([]*MetricData, 0)
	data := buf.Bytes()
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		buf := bytes.NewBuffer(data)
		dec := gob.NewDecoder(buf)
		err := dec.Decode(&out)
		if err != nil {
			panic(err)
		}
	}
}

func BenchmarkSerialize3000MetricsMsgp(b *testing.B) {
	metrics := getDifferentMetrics(3000)
	var size int
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		m := MetricDataArray(metrics)
		data, err := m.MarshalMsg(nil)
		if err != nil {
			panic(err)
		}
		size = len(data)
	}
	b.Log("final size:", size)
}
func BenchmarkDeSerialize3000MetricsMsgp(b *testing.B) {
	metrics := getDifferentMetrics(3000)
	m := MetricDataArray(metrics)
	data, err := m.MarshalMsg(nil)
	if err != nil {
	}
	var out MetricDataArray
	b.ResetTimer()
	for n := 0; n < b.N; n++ {
		_, err := out.UnmarshalMsg(data)
		if err != nil {
			panic(err)
		}
	}
}
