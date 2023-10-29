package expr

import (
	"fmt"

	"github.com/grafana/metrictank/internal/schema"
	"github.com/grafana/metrictank/pkg/api/models"
)

// Datamap contains all series to feed into the processing chain or generated therein:
// * fetched series, grouped by their expr.Req, such that expr.FuncGet can find the data it needs and feed it into subsequent expr.GraphiteFunc functions
// * additional series generated while handling the request (e.g. function processing, normalization), keyed by an empty expr.Req (such that can't be mistakenly picked up by FuncGet)
// all of these series will need to be returned to the pool once we're done with all processing and have generated our response body by calling Clean()
// eventually we'd like to be able to reuse intermediately computed data. e.g. queries like target=movingAvg(sum(foo), 10)&target=sum(foo) but for now we don't support this
type DataMap map[Req][]models.Series

func NewDataMap() DataMap {
	return make(map[Req][]models.Series)
}

func (dm DataMap) Add(r Req, s ...models.Series) {
	dm[r] = append(dm[r], s...)
}

// Clean returns all contained pointslices back to the pool.
func (dm DataMap) Clean() {
	for _, series := range dm {
		for _, serie := range series {
			pointSlicePool.Put(serie.Datapoints)
		}
	}
}

// CheckForOverlappingPoints runs through all series in the pool and makes sure there
// are no series that are overlapping (otherwise returning them would cause issues)
// This is not efficient and should probably only be called from tests
func (dm DataMap) CheckForOverlappingPoints() error {
	// First flatten series to make the logic easier
	flatseries := make([]models.Series, 0, len(dm))
	for _, series := range dm {
		flatseries = append(flatseries, series...)
	}

	// we leverage the fact here that for any given slice, if you reslice or subslice it - which may result in the
	// first slot's address changing - the last slot of the backing array remains the same and can be used
	// to find slices backed by the same array
	slicesOverlap := func(x, y []schema.Point) bool {
		return cap(x) > 0 && cap(y) > 0 && &x[0:cap(x)][cap(x)-1] ==
			&y[0:cap(y)][cap(y)-1]
	}

	for i := range flatseries {
		for j := i + 1; j < len(flatseries); j++ {
			if slicesOverlap(flatseries[i].Datapoints, flatseries[j].Datapoints) {
				return fmt.Errorf("Found overlapping series at position %d and %d", i, j)
			}
		}
	}

	return nil
}
