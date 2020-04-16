package expr

import (
	"github.com/grafana/metrictank/api/models"
)

type FuncInvert struct {
	in GraphiteFunc
}

func NewInvert() GraphiteFunc {
	return &FuncInvert{}
}

func (s *FuncInvert) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesList{val: &s.in}}, []Arg{ArgSeriesList{}}
}

func (s *FuncInvert) Context(context Context) Context {
	return context
}

func (s *FuncInvert) Exec(dataMap DataMap) ([]models.Series, error) {
	series, err := s.in.Exec(dataMap)
	if err != nil {
		return nil, err
	}
	return series, nil
}
