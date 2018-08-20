package expr

import (
	"fmt"
	"math"

	"github.com/grafana/metrictank/api/models"
	"github.com/raintank/schema"
)

type FuncKeepLastValue struct {
	in    GraphiteFunc
	limit int64
}

func NewKeepLastValue() GraphiteFunc {
	return &FuncKeepLastValue{limit: math.MaxInt64}
}

func (s *FuncKeepLastValue) Signature() ([]Arg, []Arg) {
	var stub string
	return []Arg{
			ArgSeriesList{val: &s.in},
			ArgIn{key: "limit",
				opt: true,
				args: []Arg{
					ArgInt{val: &s.limit},
					// Treats any string as infinity. This matches Graphite's behavior
					// (although intended bevahior is to let user specify "INF" as the limit)
					ArgString{val: &stub},
				},
			},
		},
		[]Arg{ArgSeriesList{}}
}

func (s *FuncKeepLastValue) Context(context Context) Context {
	return context
}

func (s *FuncKeepLastValue) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series, err := s.in.Exec(cache)
	if err != nil {
		return nil, err
	}
	limit := int(s.limit)
	outSeries := make([]models.Series, len(series))
	for i, serie := range series {
		serie.Target = fmt.Sprintf("keepLastValue(%s)", serie.Target)
		serie.QueryPatt = serie.Target

		out := pointSlicePool.Get().([]schema.Point)

		var consecutiveNaNs int
		lastVal := math.NaN()

		for i, p := range serie.Datapoints {
			out = append(out, p)
			if math.IsNaN(p.Val) {
				consecutiveNaNs++
				continue
			}
			if 0 < consecutiveNaNs && consecutiveNaNs <= limit && !math.IsNaN(lastVal) {
				for j := i - consecutiveNaNs; j < i; j++ {
					out[j].Val = lastVal
				}
			}
			consecutiveNaNs = 0
			lastVal = p.Val
		}

		if 0 < consecutiveNaNs && consecutiveNaNs <= limit && !math.IsNaN(lastVal) {
			for i := len(out) - consecutiveNaNs; i < len(out); i++ {
				out[i].Val = lastVal
			}
		}

		serie.Datapoints = out
		outSeries[i] = serie
	}
	cache[Req{}] = append(cache[Req{}], outSeries...)
	return outSeries, nil
}
