package expr

import (
	"math"

	"github.com/grafana/metrictank/api/models"
)

func crossSeriesXff(in []models.Series, index int, xFilesFactor float64) bool {
	nonNull := 0
	for i := 0; i < len(in); i++ {
		if !math.IsNaN(in[i].Datapoints[index].Val) {
			nonNull++
		}
	}
	return xff(nonNull, len(in), xFilesFactor)
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
