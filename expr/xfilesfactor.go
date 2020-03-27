package expr

import (
	"math"

	"github.com/grafana/metrictank/api/models"
)

func skipCrossSeriesXff(xFilesFactor float64) bool {
	// TODO pull default xFilesFactor from settings
	return math.IsNaN(xFilesFactor) || xFilesFactor == 0
}

// crossSeriesXff returns a boolean indicating if the []models.series
// is valid. It is valid if the ratio of non-null points to total points
// in a window (specified by index) >= minimum ratio (xFilesFactor)
func crossSeriesXff(in []models.Series, index int, xFilesFactor float64) bool {
	if skipCrossSeriesXff(xFilesFactor) {
		return true
	}
	nonNull := 0
	for i := 0; i < len(in); i++ {
		if !math.IsNaN(in[i].Datapoints[index].Val) {
			nonNull++
		}
	}
	return xff(nonNull, len(in), xFilesFactor)
}

// xff returns a boolean indicating if the ratio of non-null values
// to total values is >= minimum ratio (xFilesFactor)
func xff(nonNull int, total int, xFilesFactor float64) bool {
	if nonNull <= 0 || total <= 0 {
		return false
	}
	return float64(nonNull)/float64(total) >= xFilesFactor
}
