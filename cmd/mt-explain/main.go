package main

import (
	"fmt"

	"github.com/davecgh/go-spew/spew"
	"github.com/raintank/metrictank/expr"
)

func main() {
	target := "movingAverage(sumSeries(foo.bar), '2min')"
	exps, err := expr.ParseMany([]string{target})
	if err != nil {
		fmt.Println("Error while parsing:", err)
		return
	}
	spew.Dump(exps)

	plan, err := expr.NewPlan(exps, 1000, 1200, nil)
	if err != nil {
		fmt.Println("Error while planning", err)
		return
	}
	spew.Dump(plan)
}
