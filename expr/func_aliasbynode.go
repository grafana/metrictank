package expr

import (
	"strings"

	"github.com/raintank/metrictank/api/models"
)

type FuncAliasByNode struct {
	in    GraphiteFunc
	nodes []int64
}

func NewAliasByNode() GraphiteFunc {
	return &FuncAliasByNode{}
}

func (s *FuncAliasByNode) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesList{val: &s.in},
		ArgInts{val: &s.nodes},
	}, []Arg{ArgSeries{}}
}

func (s *FuncAliasByNode) NeedRange(from, to uint32) (uint32, uint32) {
	return from, to
}

func (s *FuncAliasByNode) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series, err := s.in.Exec(cache)
	if err != nil {
		return nil, err
	}
	for i, serie := range series {
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
		series[i].Target = strings.Join(name, ".")
	}
	return series, nil
}
