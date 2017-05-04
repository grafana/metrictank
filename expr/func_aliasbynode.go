package expr

import (
	"strings"

	"github.com/raintank/metrictank/api/models"
)

type FuncAliasByNode struct {
	nodes []int
}

func NewAliasByNode() Func {
	return &FuncAliasByNode{}
}

func (s *FuncAliasByNode) Signature() ([]argType, []optArg, []argType) {
	return []argType{seriesLists, integers}, nil, []argType{series}
}

func (s *FuncAliasByNode) Init(args []*expr, namedArgs map[string]*expr) error {
	for _, i := range args[1:] {
		s.nodes = append(s.nodes, int(i.int))
	}
	return nil
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
		for _, n := range s.nodes {
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
