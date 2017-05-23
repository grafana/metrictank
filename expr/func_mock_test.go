package expr

import "github.com/raintank/metrictank/api/models"

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

func (s FuncMock) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	return s.data, nil
}
