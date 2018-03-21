package msg

import (
	"math"
	"reflect"
	"testing"

	schema "gopkg.in/raintank/schema.v1"
)

func TestWriteReadPointMsg(t *testing.T) {
	mp := schema.MetricPoint{
		MKey: schema.MKey{
			Org: 123,
		},
		Time:  math.MaxUint32,
		Value: 123.45,
	}
	buf := make([]byte, 0, 33)
	out, err := WritePointMsg(mp, buf, FormatMetricPoint)
	if err != nil {
		t.Fatalf("%s", err.Error())
	}

	if !IsPointMsg(out) {
		t.Fatal("IsPointMsg: exp true, got false")
	}

	leftover, outPoint, err := ReadPointMsg(out, 6)
	if err != nil {
		t.Fatalf("%s", err.Error())
	}
	if len(leftover) > 0 {
		t.Fatalf("expected no leftover. got %v", leftover)
	}

	if !reflect.DeepEqual(mp, outPoint) {
		t.Fatalf("expected point %v, got %v", mp, outPoint)
	}
}

func TestWriteReadPointMsgWithoutOrg(t *testing.T) {
	mp := schema.MetricPoint{
		MKey: schema.MKey{
			Org: 123,
		},
		Time:  math.MaxUint32,
		Value: 123.45,
	}
	buf := make([]byte, 0, 29)
	out, err := WritePointMsg(mp, buf, FormatMetricPointWithoutOrg)
	if err != nil {
		t.Fatalf("%s", err.Error())
	}

	if !IsPointMsg(out) {
		t.Fatal("IsPointMsg: exp true, got false")
	}

	exp := mp
	exp.MKey.Org = 6 // ReadPointMsg will have to set the default org and we want to check it
	leftover, outPoint, err := ReadPointMsg(out, 6)
	if err != nil {
		t.Fatalf("%s", err.Error())
	}
	if len(leftover) > 0 {
		t.Fatalf("expected no leftover. got %v", leftover)
	}

	if !reflect.DeepEqual(exp, outPoint) {
		t.Fatalf("expected point %v, got %v", exp, outPoint)
	}
}
