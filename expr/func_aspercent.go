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
	if s.nodes != nil {

		// List of keys
		var keys []string
		// Series grouped by key
		metaSeries := groupSeriesByKey(series, s.nodes, &keys)
		// The totals series for each key
		var totalSeries map[string]models.Series

		// calculate the sum
		if s.totalFloat == math.NaN() && s.totalSeries == nil {
			totalSeries = getTotalSeries(metaSeries)
			//TODO check if serieslist
			// calculate sum of totals series
		} else if s.totalSeries != nil {
			total, err := s.totalSeries.Exec(cache)
			if err != nil {
				return nil, err
			}
			totalSeriesLists := groupSeriesByKey(total, s.nodes, &keys)
			totalSeries = getTotalSeries(totalSeriesLists)
		} else {
			return nil, errors.New("total must be None or a seriesList")
		}

		for _, key := range keys {
			// No series for total series
			if _, ok := metaSeries[key]; !ok {
				serie2 := totalSeries[key]
				name := fmt.Sprintf("asPercent(MISSING,%s)", serie2.QueryPatt)
				serie2.QueryPatt = name
				serie2.Target = name
				serie2.Tags = map[string]string{"name": name}
				for i := range serie2.Datapoints {
					serie2.Datapoints[i].Val = math.NaN()
				}
				outSeries = append(outSeries, serie2)
				cache[Req{}] = append(cache[Req{}], serie2)
				continue
			}

			for _, serie1 := range metaSeries[key] {
				// no total
				if _, ok := totalSeries[key]; !ok {
					name := fmt.Sprintf("asPercent(%s,MISSING)", serie1.QueryPatt)
					serie1.QueryPatt = name
					serie1.Target = name
					serie1.Tags = map[string]string{"name": name}
					for i := range serie1.Datapoints {
						serie1.Datapoints[i].Val = math.NaN()
					}
				} else {
					serie2 := totalSeries[key]
					name := fmt.Sprintf("asPercent(%s,%s)", serie1.QueryPatt, serie2.QueryPatt)
					serie1.QueryPatt = name
					serie1.Target = name
					serie1.Tags = map[string]string{"name": name}
					for i := range serie1.Datapoints {
						if serie2.Datapoints[i].Val == 0 {
							serie1.Datapoints[i].Val = math.NaN()
						} else {
							serie1.Datapoints[i].Val = serie1.Datapoints[i].Val / serie2.Datapoints[i].Val * 100
						}
					}
				}
				outSeries = append(outSeries, serie1)
				cache[Req{}] = append(cache[Req{}], serie1)
			}
		}
		return outSeries, nil
	}

	var totalsSerie models.Series
	if math.IsNaN(s.totalFloat) && s.totalSeries == nil {
		totalsSerie = sumSeries(series)
	} else if s.totalSeries != nil {
		total, err := s.totalSeries.Exec(cache)
		if err != nil {
			return nil, err
		}
		if len(total) == 1 {
			totalsSerie = total[0]
		} else if len(total) == len(series) {
			sort.Sort(sortByTarget(series))
			sort.Sort(sortByTarget(total))
			for i, serie1 := range series {
				serie2 := total[i]
				serie1.QueryPatt = fmt.Sprintf("asPercent(%s,%s)", serie1.QueryPatt, serie2.QueryPatt)
				serie1.Target = fmt.Sprintf("asPercent(%s,%s)", serie1.Target, serie2.Target)
				serie1.Tags = map[string]string{"name": serie1.Target}
				for i := range serie1.Datapoints {
					if serie2.Datapoints[i].Val == 0 {
						serie1.Datapoints[i].Val = math.NaN()
					} else {
						serie1.Datapoints[i].Val = serie1.Datapoints[i].Val / serie2.Datapoints[i].Val * 100
					}
				}
				outSeries = append(outSeries, serie1)
				cache[Req{}] = append(cache[Req{}], serie1)
			}
			return outSeries, nil
		} else {
			return nil, errors.New("asPercent second argument must be missing, a single digit, reference exactly 1 series or reference the same number of series as the first argument")
		}
	} else {
		totalsSerie.QueryPatt = fmt.Sprint(s.totalFloat)
	}

	for _, serie := range series {
		serie.QueryPatt = fmt.Sprintf("asPercent(%s,%s)", serie.QueryPatt, totalsSerie.QueryPatt)
		serie.Target = fmt.Sprintf("asPercent(%s,%s)", serie.Target, totalsSerie.QueryPatt)
		serie.Tags = map[string]string{"name": serie.Target}
		for i := range serie.Datapoints {
			var total float64
			if len(totalsSerie.Datapoints) > i {
				total = totalsSerie.Datapoints[i].Val
			} else {
				total = s.totalFloat
			}
			serie.Datapoints[i].Val = computeAsPercent(serie.Datapoints[i].Val, total)
		}
		outSeries = append(outSeries, serie)
		cache[Req{}] = append(cache[Req{}], serie)
	}

	return outSeries, nil
}

func computeAsPercent(in, total float64) float64 {
	if math.IsNaN(in) || math.IsNaN(total) {
		return math.NaN()
	}
	if total == 0 {
		if in == 0 {
			return math.NaN()
		}
		return math.MaxFloat64
	}
	return 100 * in / total
}

func groupSeriesByKey(series []models.Series, nodes []expr, keys *[]string) map[string][]models.Series {
	keyedSeries := make(map[string][]models.Series)

	for _, serie := range series {
		key := aggKey(serie, nodes)
		if _, ok := keyedSeries[key]; !ok {
			keyedSeries[key] = []models.Series{serie}
			for _, k := range *keys {
				if k == key {
					continue
				}
			}
			*keys = append(*keys, key)
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
func sumSeries(series []models.Series) models.Series {
	var summedSerie models.Series
	if len(series) == 1 {
		summedSerie = series[0]
		summedSerie.Target = fmt.Sprintf("sumSeries(%s)", summedSerie.Target)
		summedSerie.QueryPatt = fmt.Sprintf("sumSeries(%s)", summedSerie.QueryPatt)
		summedSerie.Tags = map[string]string{"name": summedSerie.Target}
	} else {
		out := pointSlicePool.Get().([]schema.Point)
		crossSeriesSum(series, &out)
		queryPatts := make([]string, len(series))
		for i, v := range series {
			queryPatts[i] = v.QueryPatt
		}
		name := fmt.Sprintf("sumSeries(%s)", strings.Join(queryPatts, ","))
		cons, queryCons := summarizeCons(series)
		summedSerie = models.Series{
			Target:       name,
			QueryPatt:    name,
			Datapoints:   out,
			Interval:     series[0].Interval,
			Consolidator: cons,
			QueryCons:    queryCons,
			Tags:         map[string]string{"name": name},
		}
	}
	return summedSerie
}

func aggKey(serie models.Series, nodes []expr) string {
	metric := extractMetric(serie.Target)
	if len(metric) == 0 {
		metric = serie.Tags["name"]
	}
	// Trim off tags (if they are there) and split on '.'
	parts := strings.Split(strings.SplitN(metric, ";", 2)[0], ".")
	var name []string
	for _, n := range nodes {
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
	return strings.Join(name, ".")
}

type sortByTarget []models.Series

func (s sortByTarget) Len() int {
	return len(s)
}
func (s sortByTarget) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}
func (s sortByTarget) Less(i, j int) bool {
	return s[i].Target < s[j].Target
}
