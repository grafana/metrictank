package expr

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/grafana/metrictank/api/models"
	schema "gopkg.in/raintank/schema.v1"
)

type FuncAsPercent struct {
	in          GraphiteFunc
	totalFloat  float64
	totalSeries GraphiteFunc
	nodes       []expr
}

func NewAsPercent() GraphiteFunc {
	return &FuncAsPercent{totalFloat: math.NaN()}
}

func (s *FuncAsPercent) Signature() ([]Arg, []Arg) {
	return []Arg{
		ArgSeriesList{val: &s.in},
		ArgIn{
			key: "total",
			opt: true,
			args: []Arg{
				ArgFloat{val: &s.totalFloat},
				ArgSeriesList{val: &s.totalSeries},
			},
		},
		ArgStringsOrInts{val: &s.nodes, opt: true, key: "nodes"},
	}, []Arg{ArgSeriesList{}}
}

func (s *FuncAsPercent) Context(context Context) Context {
	return context
}

func (s *FuncAsPercent) Exec(cache map[Req][]models.Series) ([]models.Series, error) {
	series, err := s.in.Exec(cache)
	if err != nil {
		return nil, err
	}

	var outSeries []models.Series
	defer func() { cache[Req{}] = append(cache[Req{}], outSeries...) }()
	var totals []models.Series
	if s.totalSeries != nil {
		totals, err = s.totalSeries.Exec(cache)
		if err != nil {
			return nil, err
		}
	}

	if s.nodes != nil {
		outSeries, err = s.execWithNodes(series, totals)
	} else {
		outSeries, err = s.execWithoutNodes(series, totals)
	}
	return outSeries, err
}

func (s *FuncAsPercent) execWithNodes(series, totals []models.Series) ([]models.Series, error) {
	var outSeries []models.Series
	// Set of keys
	keys := make(map[string]struct{})
	// Series grouped by key
	metaSeries := groupSeriesByKey(series, s.nodes, &keys)
	// The totals series for each key
	var totalSeries map[string]models.Series

	// calculate the sum
	if math.IsNaN(s.totalFloat) && totals == nil {
		totalSeries = getTotalSeries(metaSeries)
		// calculate sum of totals series
	} else if totals != nil {
		totalSeriesLists := groupSeriesByKey(totals, s.nodes, &keys)
		totalSeries = getTotalSeries(totalSeriesLists)
	} else {
		return nil, errors.New("total must be None or a seriesList")
	}

	for key := range keys {
		// No input series for a corresponding total series
		if _, ok := metaSeries[key]; !ok {
			serie2 := totalSeries[key].Copy(pointSlicePool.Get().([]schema.Point))
			serie2.QueryPatt = fmt.Sprintf("asPercent(MISSING,%s)", serie2.QueryPatt)
			serie2.Target = fmt.Sprintf("asPercent(MISSING,%s)", serie2.Target)
			serie2.Tags = map[string]string{"name": serie2.Target}
			for i := range serie2.Datapoints {
				serie2.Datapoints[i].Val = math.NaN()
			}
			outSeries = append(outSeries, serie2)
			continue
		}

		for _, serie1 := range metaSeries[key] {
			serie1 = serie1.Copy(pointSlicePool.Get().([]schema.Point))
			// No total series for a corresponding input series
			if _, ok := totalSeries[key]; !ok {
				serie1.QueryPatt = fmt.Sprintf("asPercent(%s,MISSING)", serie1.QueryPatt)
				serie1.Target = fmt.Sprintf("asPercent(%s,MISSING)", serie1.Target)
				serie1.Tags = map[string]string{"name": serie1.Target}
				for i := range serie1.Datapoints {
					serie1.Datapoints[i].Val = math.NaN()
				}
			} else {
				serie2 := totalSeries[key]
				serie1.QueryPatt = fmt.Sprintf("asPercent(%s,%s)", serie1.QueryPatt, serie2.QueryPatt)
				serie1.Target = fmt.Sprintf("asPercent(%s,%s)", serie1.Target, serie2.Target)
				serie1.Tags = map[string]string{"name": serie1.Target}
				for i := range serie1.Datapoints {
					serie1.Datapoints[i].Val = computeAsPercent(serie1.Datapoints[i].Val, serie2.Datapoints[i].Val)
				}
			}
			outSeries = append(outSeries, serie1)
		}
	}
	return outSeries, nil
}

func (s *FuncAsPercent) execWithoutNodes(series, totals []models.Series) ([]models.Series, error) {
	var outSeries []models.Series
	var totalsSerie models.Series
	if math.IsNaN(s.totalFloat) && totals == nil {
		totalsSerie = sumSeries(series)
		if len(series) == 1 {
			totalsSerie.Target = fmt.Sprintf("sumSeries(%s)", totalsSerie.Target)
			totalsSerie.QueryPatt = fmt.Sprintf("sumSeries(%s)", totalsSerie.QueryPatt)
			totalsSerie.Tags = map[string]string{"name": totalsSerie.Target}
		}
	} else if totals != nil {
		if len(totals) == 1 {
			totalsSerie = totals[0]
		} else if len(totals) == len(series) {
			// Sorted to match the input series with the total series based on Target.
			// Mimicks Graphite's implementation
			sort.Slice(series, func(i, j int) bool {
				return series[i].Target < series[j].Target
			})
			sort.Slice(totals, func(i, j int) bool {
				return totals[i].Target < totals[j].Target
			})
			for i, serie1 := range series {
				serie2 := totals[i]
				serie1 = serie1.Copy(pointSlicePool.Get().([]schema.Point))
				serie1.QueryPatt = fmt.Sprintf("asPercent(%s,%s)", serie1.QueryPatt, serie2.QueryPatt)
				serie1.Target = fmt.Sprintf("asPercent(%s,%s)", serie1.Target, serie2.Target)
				serie1.Tags = map[string]string{"name": serie1.Target}
				for i := range serie1.Datapoints {
					serie1.Datapoints[i].Val = computeAsPercent(serie1.Datapoints[i].Val, serie2.Datapoints[i].Val)
				}
				outSeries = append(outSeries, serie1)
			}
			return outSeries, nil
		} else {
			return nil, errors.New("asPercent second argument must be missing, a single digit, reference exactly 1 series or reference the same number of series as the first argument")
		}
	} else {
		totalsSerie.QueryPatt = fmt.Sprint(s.totalFloat)
	}

	for _, serie := range series {
		serie = serie.Copy(pointSlicePool.Get().([]schema.Point))
		serie.QueryPatt = fmt.Sprintf("asPercent(%s,%s)", serie.QueryPatt, totalsSerie.QueryPatt)
		serie.Target = fmt.Sprintf("asPercent(%s,%s)", serie.Target, totalsSerie.QueryPatt)
		serie.Tags = map[string]string{"name": serie.Target}
		for i := range serie.Datapoints {
			var totalVal float64
			if len(totalsSerie.Datapoints) > i {
				totalVal = totalsSerie.Datapoints[i].Val
			} else {
				totalVal = s.totalFloat
			}
			serie.Datapoints[i].Val = computeAsPercent(serie.Datapoints[i].Val, totalVal)
		}
		outSeries = append(outSeries, serie)
	}
	return outSeries, nil
}

func computeAsPercent(in, total float64) float64 {
	if math.IsNaN(in) || math.IsNaN(total) {
		return math.NaN()
	}
	if total == 0 {
		return math.NaN()
	}
	return in / total * 100
}

func groupSeriesByKey(series []models.Series, nodes []expr, keys *map[string]struct{}) map[string][]models.Series {
	keyedSeries := make(map[string][]models.Series)
	for _, serie := range series {
		key := aggKey(serie, nodes)
		if _, ok := keyedSeries[key]; !ok {
			keyedSeries[key] = []models.Series{serie}
			(*keys)[key] = struct{}{}
		} else {
			keyedSeries[key] = append(keyedSeries[key], serie)
		}
	}
	return keyedSeries
}

// Sums each seriesList in map of seriesLists
func getTotalSeries(totalSeriesLists map[string][]models.Series) map[string]models.Series {
	totalSeries := make(map[string]models.Series, len(totalSeriesLists))
	for key := range totalSeriesLists {
		totalSeries[key] = sumSeries(totalSeriesLists[key])
	}
	return totalSeries
}

// Sums seriesList
// Datapoints are always a copy
func sumSeries(series []models.Series) models.Series {
	if len(series) == 1 {
		return series[0]
	}
	out := pointSlicePool.Get().([]schema.Point)
	crossSeriesSum(series, &out)
	var queryPatts []string

Loop:
	for _, v := range series {
		// avoid duplicates
		for _, qp := range queryPatts {
			if qp == v.QueryPatt {
				continue Loop
			}
		}
		queryPatts = append(queryPatts, v.QueryPatt)
	}
	name := fmt.Sprintf("sumSeries(%s)", strings.Join(queryPatts, ","))
	cons, queryCons := summarizeCons(series)
	return models.Series{
		Target:       name,
		QueryPatt:    name,
		Datapoints:   out,
		Interval:     series[0].Interval,
		Consolidator: cons,
		QueryCons:    queryCons,
		Tags:         map[string]string{"name": name},
	}
}
