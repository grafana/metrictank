package seriescycle

import (
	"github.com/grafana/metrictank/api/models"
)

// SeriesCycler manages what should happen to a models.Series
// when we create it, or drop our reference to it
// Warning: this is for managing recycling of the Datapoints field.
// In some places we create "new" series by repurposing existing
// Series and adding a new Datapoints slice onto it.
// E.g. in expr.NormalizeTo
type SeriesCycler struct {
	New  func(models.Series)
	Done func(models.Series)
}

var NullCycler = SeriesCycler{
	New: func(in models.Series) {
	},
	Done: func(in models.Series) {
	},
}
