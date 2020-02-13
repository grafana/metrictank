package expr

import "github.com/grafana/metrictank/api/models"

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
			pointSlicePool.Put(serie.Datapoints[:0])
		}
	}
}
