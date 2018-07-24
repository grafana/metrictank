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
	var totals []models.Series
	if s.totalSeries != nil {
		totals, err = s.totalSeries.Exec(cache)
		if err != nil {
			return nil, err
		}
		if len(totals) == 0 {
			totals = nil
		}
	}

	if s.nodes != nil {
		// List of keys
		var keys []string
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

		for _, key := range keys {
			fmt.Printf("Key: %v", key)
			// No series for total series
			if _, ok := metaSeries[key]; !ok {
				serie2 := totalSeries[key]
				serie2.QueryPatt = fmt.Sprintf("asPercent(MISSING,%s)", serie2.QueryPatt)
				serie2.Target = fmt.Sprintf("asPercent(MISSING,%s)", serie2.Target)
				serie2.Tags = map[string]string{"name": serie2.Target}
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
				cache[Req{}] = append(cache[Req{}], serie1)
			}
		}
		return outSeries, nil
	}

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
			sort.Sort(sortByTarget(series))
			sort.Sort(sortByTarget(totals))
			for i, serie1 := range series {
				serie2 := totals[i]
				serie1.QueryPatt = fmt.Sprintf("asPercent(%s,%s)", serie1.QueryPatt, serie2.QueryPatt)
				serie1.Target = fmt.Sprintf("asPercent(%s,%s)", serie1.Target, serie2.Target)
				serie1.Tags = map[string]string{"name": serie1.Target}
				for i := range serie1.Datapoints {
					serie1.Datapoints[i].Val = computeAsPercent(serie1.Datapoints[i].Val, serie2.Datapoints[i].Val)
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
			var totalVal float64
			if len(totalsSerie.Datapoints) > i {
				totalVal = totalsSerie.Datapoints[i].Val
			} else {
				totalVal = s.totalFloat
			}
			serie.Datapoints[i].Val = computeAsPercent(serie.Datapoints[i].Val, totalVal)
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
		return math.NaN()
	}
	return in / total * 100
}

func groupSeriesByKey(series []models.Series, nodes []expr, keys *[]string) map[string][]models.Series {
	keyedSeries := make(map[string][]models.Series)
Loop:
	for _, serie := range series {
		key := aggKey(serie, nodes)
		if _, ok := keyedSeries[key]; !ok {
			keyedSeries[key] = []models.Series{serie}
			for _, k := range *keys {
				if k == key {
					continue Loop
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
	} else {
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
