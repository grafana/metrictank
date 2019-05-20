package expr

import (
	"strings"

	"github.com/grafana/metrictank/api/models"
)

type FuncSubstr struct {
	in    GraphiteFunc
	start int64
	stop  int64
}

func NewSubstr() GraphiteFunc {
	return &FuncSubstr{}
}

func (s *FuncSubstr) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesList{val: &s.in},
		ArgInt{val: &s.start, opt: true, key: "start"},
		ArgInt{val: &s.stop, opt: true, key: "stop"},
	}, []Arg{ArgSeriesList{}}
}

func (s *FuncSubstr) Context(context Context) Context {
	return context
}

func (s *FuncSubstr) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series, err := s.in.Exec(cache)
	if err != nil {
		return nil, err
	}

	out := make([]models.Series, len(series))
	for i, serie := range series {
		out[i] = serie
		left := strings.LastIndex(serie.Target, "(") + 1
		right := strings.Index(serie.Target, ")")
		if right < 0 {
			right = len(serie.Target) + 1
		}
		cleanName := serie.Target[left:right]
		cleanName = strings.SplitN(cleanName, ",", 2)[0]

		var name string
		if s.stop == 0 {
			name = strings.Join(strings.Split(cleanName, ".")[s.start:], ".")
		} else {
			name = strings.Join(strings.Split(cleanName, ".")[s.start:s.stop], ".")
		}

		serie.Target = name
		serie.QueryPatt = name
		cache[Req{}] = append(cache[Req{}], serie)
	}

	return out, nil
}
