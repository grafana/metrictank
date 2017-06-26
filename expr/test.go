package expr

import (
	"fmt"
	"math"

	"github.com/raintank/metrictank/api/models"
)

var results []models.Series

// cannot just use reflect.DeepEqual because NaN != NaN, whereas we want NaN == NaN
// https://github.com/golang/go/issues/12025
func equalSeries(exp, got models.Series) error {
	if got.Target != exp.Target {
		return fmt.Errorf("Target %q, got %q", exp.Target, got.Target)
	}
	if got.Interval != exp.Interval {
		return fmt.Errorf("Interval %d, got %d", exp.Interval, got.Interval)
	}
	if got.QueryPatt != exp.QueryPatt {
		return fmt.Errorf("QueryPatt %q, got %q", exp.QueryPatt, got.QueryPatt)
	}
	if got.QueryFrom != exp.QueryFrom {
		return fmt.Errorf("QueryFrom %d, got %d", exp.QueryFrom, got.QueryFrom)
	}
	if got.QueryTo != exp.QueryTo {
		return fmt.Errorf("QueryTo %d, got %d", exp.QueryTo, got.QueryTo)
	}
	if got.QueryCons != exp.QueryCons {
		return fmt.Errorf("QueryCons %v, got %v", exp.QueryCons, got.QueryCons)
	}
	if got.Consolidator != exp.Consolidator {
		return fmt.Errorf("Consolidator %v, got %v", exp.Consolidator, got.Consolidator)
	}
	if len(got.Datapoints) != len(exp.Datapoints) {
		return fmt.Errorf("output expected %d, got %d", len(exp.Datapoints), len(got.Datapoints))
	}
	for j, p := range got.Datapoints {
		bothNaN := math.IsNaN(p.Val) && math.IsNaN(exp.Datapoints[j].Val)
		if (bothNaN || p.Val == exp.Datapoints[j].Val) && p.Ts == exp.Datapoints[j].Ts {
			continue
		}
		return fmt.Errorf("point %d - expected %v got %v", j, exp.Datapoints[j], p)
	}
	return nil
}
