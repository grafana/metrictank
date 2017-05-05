package expr

import (
	"strings"

	"github.com/raintank/metrictank/api/models"
)

type FuncAliasByNode struct {
	nodes []int64
}

func NewAliasByNode() Func {
	return &FuncAliasByNode{}
}

func (s *FuncAliasByNode) Signature() ([]arg, []arg) {
	return []arg{
		argSeriesLists{},
		argInts{store: &s.nodes},
	}, []arg{argSeries{}}
}

func (s *FuncAliasByNode) NeedRange(from, to uint32) (uint32, uint32) {
	return from, to
}

func (s *FuncAliasByNode) Exec(cache map[Req][]models.Series, named map[string]interface{}, inputs ...interface{}) ([]interface{}, error) {
	var series []models.Series
	var out []interface{}
	for _, input := range inputs {
		seriesList, ok := input.([]models.Series)
		if !ok {
			break
		}
		series = append(series, seriesList...)

	}
	for _, serie := range series {
		metric := extractMetric(serie.Target)
		parts := strings.Split(metric, ".")
		var name []string
		for _, n64 := range s.nodes {
			n := int(n64)
			if n < 0 {
				n += len(parts)
			}
			if n >= len(parts) || n < 0 {
				continue
			}
			name = append(name, parts[n])
		}

		out = append(out, models.Series{
			Target:     strings.Join(name, "."),
			Datapoints: serie.Datapoints,
			Interval:   serie.Interval,
		})
	}
	return out, nil
}
