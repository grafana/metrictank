package expr

import (
	"math"

	"github.com/raintank/schema"
)

func valuesXff(values []schema.Point, xFilesFactor float64) bool {
	nonNull := 0
	for _, v := range values {
		if !math.IsNaN(v.Val) {
			nonNull++
		}
	}
	return xff(nonNull, len(values), xFilesFactor)
}

func xff(nonNull int, total int, xFilesFactor float64) bool {
	if nonNull <= 0 || total <= 0 {
		return false
	}
	if math.IsNaN(xFilesFactor) {
		xFilesFactor = 0 // TODO pull default xFilesFactor from settings
	}
	return float64(nonNull)/float64(total) >= xFilesFactor
}
