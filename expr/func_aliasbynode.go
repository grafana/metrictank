package expr

import (
	"strings"

	"github.com/grafana/metrictank/api/models"
)

type FuncAliasByNode struct {
	in    GraphiteFunc
	nodes []expr
}

func NewAliasByNode() GraphiteFunc {
	return &FuncAliasByNode{}
}

func (s *FuncAliasByNode) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesList{val: &s.in},
		ArgStringsOrInts{val: &s.nodes},
	}, []Arg{ArgSeries{}}
}

func (s *FuncAliasByNode) Context(context Context) Context {
	return context
}

func (s *FuncAliasByNode) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series, err := s.in.Exec(cache)
	if err != nil {
		return nil, err
	}
	for i, serie := range series {
		// Extract metric may not find a target if `seriesByTag` was used.
		// If so, then we can try to grab the "name" tag.
		metric := extractMetric(serie.Target)
		if len(metric) == 0 {
			metric = serie.Tags["name"]
		}
		// Trim off tags (if they are there) and split on '.'
		parts := strings.Split(strings.SplitN(metric, ";", 2)[0], ".")
		var name []string
		for _, n := range s.nodes {
			if n.etype == etInt {
				idx := int(n.int)
				if idx < 0 {
					idx += len(parts)
				}
				if idx >= len(parts) || idx < 0 {
					continue
				}
				name = append(name, parts[idx])
			} else if n.etype == etString {
				s := n.str
				name = append(name, serie.Tags[s])
			}
		}
		n := strings.Join(name, ".")
		series[i].Target = n
		series[i].QueryPatt = n
	}
	return series, nil
}
