package expr

import "github.com/grafana/metrictank/api/models"

// internal function just for getting data
type FuncMock struct {
	data []models.Series
}

func NewMock(data []models.Series) GraphiteFunc {
	return FuncMock{data}
}

func (s FuncMock) Signature() ([]Arg, []Arg) {
	return nil, []Arg{ArgSeries{}}
}

func (s FuncMock) Context(context Context) Context {
	return context
}

func (s FuncMock) Exec(dataMap DataMap) ([]models.Series, error) {
	// func_get would retrieve this from the map (added at a higher layer)
	// Mock should add `data` to properly mock the render path
	dataMap.Add(Req{}, s.data...)
	for i := range s.data {
		s.data[i].SetTags()
	}

	return s.data, nil
}
