package expr

import (
	"math"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
)

func skipCrossSeriesXff(xFilesFactor float64) bool {
	// TODO pull default xFilesFactor from settings
	return math.IsNaN(xFilesFactor) || xFilesFactor == 0
}

// crossSeriesXff returns a boolean indicating whether the set of points
// at the given index across all input series is valid.
// It is valid if the ratio of non-null points to total points
// >= minimum ratio (xFilesFactor)
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

// pointsXffCheck returns a boolean indicating whether the set of points
// is valid or not
// It is valid if the ratio of non-null points to total points
// >= minimum ratio (xFilesFactor)
func pointsXffCheck(in []schema.Point, xFilesFactor float64) bool {
	notNull := 0
	for _, p := range in {
		if !math.IsNaN(p.Val) {
			notNull++
		}
	}
	return xff(notNull, len(in), xFilesFactor)
}

// xff returns a boolean indicating if the ratio of non-null values
// to total values is >= minimum ratio (xFilesFactor)
func xff(nonNull int, total int, xFilesFactor float64) bool {
	if nonNull <= 0 || total <= 0 {
		return false
	}
	return float64(nonNull)/float64(total) >= xFilesFactor
}
