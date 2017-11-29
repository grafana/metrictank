package expr

import (
	"strings"

	"github.com/grafana/metrictank/api/models"
)

// internal function just for getting data
type FuncGet struct {
	req Req
}

func NewGet(req Req) GraphiteFunc {
	return FuncGet{req}
}

func (s FuncGet) Signature() ([]Arg, []Arg) {
	return nil, []Arg{ArgSeries{}}
}

func (s FuncGet) Context(context Context) Context {
	return context
}

func (s FuncGet) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series := cache[s.req]

	if len(series) == 0 {
		return series, nil
	}

	var out []models.Series

	for _, serie := range series {
		tagSplits := strings.Split(serie.Target, ";")

		if serie.Tags == nil {
			serie.Tags = make(map[string]string, len(tagSplits))
		}

		for _, tagPair := range tagSplits[1:] {
			parts := strings.SplitN(tagPair, "=", 2)
			if len(parts) != 2 {
				// Shouldn't happen
				continue
			}
			serie.Tags[parts[0]] = parts[1]
		}
		serie.Tags["name"] = tagSplits[0]
		out = append(out, serie)
	}

	return out, nil
}
