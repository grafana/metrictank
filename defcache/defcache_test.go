package defcache

import (
	"github.com/raintank/met/helper"
	"github.com/raintank/metrictank/metricdef"
	"gopkg.in/raintank/schema.v0"
	"testing"
)

func TestAdd(t *testing.T) {
	stats, _ := helper.New(false, "", "standard", "metrics_tank", "")
	defCache := New(metricdef.NewDefsMock(), stats)

	assert := func(def *schema.MetricDefinition, id string, orgId int, name, metric string, interval int, unit, tt, tag1, tag2 string) {
		if def.Id != id {
			t.Fatalf("bad Id %q", def.Id)
		}
		if def.OrgId != orgId {
			t.Fatalf("bad OrgId %q", def.OrgId)
		}
		if def.Name != name {
			t.Fatalf("bad Name %q", def.Name)
		}
		if def.Metric != metric {
			t.Fatalf("bad id %q", def.Metric)
		}
		if def.Interval != interval {
			t.Fatalf("bad Interval %q", def.Interval)
		}
		if def.Unit != unit {
			t.Fatalf("bad Unit %q", def.Unit)
		}
		if def.TargetType != tt {
			t.Fatalf("bad TargetType %q", def.TargetType)
		}
		if len(def.Tags) != 2 || def.Tags[0] != tag1 || def.Tags[1] != tag2 {
			t.Fatalf("bad Tags %q", def.Tags)
		}
	}

	def := defCache.Get("id")
	if def != nil {
		t.Fatalf("lookup for 'id' should return no results")
	}
	m := &schema.MetricData{
		Id:         "id",
		OrgId:      1,
		Name:       "name",
		Metric:     "metric",
		Interval:   10,
		Value:      1.5,
		Unit:       "unit",
		Time:       1234567890,
		TargetType: "gauge",
		Tags:       []string{"tag1", "tag2"},
	}

	defCache.Add(m)
	def = defCache.Get("id")
	if def == nil {
		t.Fatalf("lookup for 'id' should return a result")
	}
	assert(def, "id", 1, "name", "metric", 10, "unit", "gauge", "tag1", "tag2")

	m.Id = "id2"
	m.Name = "name2"
	m.Tags[0] = "taga"
	m.Tags[1] = "tagb"
	defCache.Add(m)
	def = defCache.Get("id2")
	if def == nil {
		t.Fatalf("lookup for 'id2' should return a result")
	}
	assert(def, "id2", 1, "name2", "metric", 10, "unit", "gauge", "taga", "tagb")
	def = defCache.Get("id")
	if def == nil {
		t.Fatalf("lookup for 'id' should return a result")
	}
	assert(def, "id", 1, "name", "metric", 10, "unit", "gauge", "tag1", "tag2")
}
