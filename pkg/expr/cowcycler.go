package expr

import (
	"github.com/grafana/metrictank/pkg/api/models"
	"github.com/grafana/metrictank/pkg/api/seriescycle"
)

// NewCOW returns a SeriesCycler tailored towards COW scoped to a user request
// * newly provisioned series go into the datamap, such that the datamap can reclaim them when the request finishes
// * discarded series are left alone (not recycled, because they may be read elsewhere)
// (keeping this in expr to keep calls from expr code short, and avoid import cycle)
func NewCOWCycler(dm DataMap) seriescycle.SeriesCycler {
	return seriescycle.SeriesCycler{
		New: func(in models.Series) {
			dm.Add(Req{}, in)
		},
		Done: func(in models.Series) {
		},
	}
}
