package expr

import (
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/errors"
	"github.com/grafana/metrictank/schema"
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
	in, err := s.in.Exec(cache)
	if err != nil {
		return nil, err
	}

	var totals []models.Series
	if s.totalSeries != nil {
		totals, err = s.totalSeries.Exec(cache)
		if err != nil {
			return nil, err
		}
	}

	if s.nodes != nil {
		if !math.IsNaN(s.totalFloat) {
			return nil, errors.NewBadRequest("total must be None or a seriesList")
		}
		return s.execWithNodes(in, totals, cache)
	}

	if totals != nil && len(totals) != 1 && len(totals) != len(in) {
		return nil, errors.NewBadRequest("if nodes specified, asPercent second argument (total) must be missing, a single digit, reference exactly 1 series or reference the same number of series as the first argument")
	}
	return s.execWithoutNodes(in, totals, cache)
}

// when nodes are given, totals can be:
// * nil -> in which case we divide by the sum of all input series in the group
// * serieslist -> we will sum the series in the group (or not, if we know that the group won't exist in `in` anyway, we don't need to do this work)
// * NOT a number in this case.
func (s *FuncAsPercent) execWithNodes(in, totals []models.Series, cache map[Req][]models.Series) ([]models.Series, error) {
	var outSeries []models.Series

	keys := make(map[string]struct{}) // will track all aggKeys seen, amongst inputs and totals series
	inByKey := groupSeriesByKey(in, s.nodes, keys)
	var totalSerieByKey map[string]models.Series

	// calculate the sum

	if math.IsNaN(s.totalFloat) && totals == nil {
		totalSerieByKey = getTotalSeries(inByKey, inByKey, cache)
	} else if totals != nil {
		totalSeriesByKey := groupSeriesByKey(totals, s.nodes, keys)
		totalSerieByKey = getTotalSeries(totalSeriesByKey, inByKey, cache)
	}

	var nones []schema.Point

	for key := range keys {
		// No input series for a corresponding total series
		if _, ok := inByKey[key]; !ok {
			nonesSerie := totalSerieByKey[key]
			nonesSerie.QueryPatt = fmt.Sprintf("asPercent(MISSING,%s)", totalSerieByKey[key].QueryPatt)
			nonesSerie.Target = fmt.Sprintf("asPercent(MISSING,%s)", totalSerieByKey[key].Target)
			nonesSerie.Tags = map[string]string{"name": nonesSerie.Target}

			if nones == nil {
				nones = pointSlicePool.Get().([]schema.Point)
				for _, p := range totalSerieByKey[key].Datapoints {
					p.Val = math.NaN()
					nones = append(nones, p)
				}
				cache[Req{}] = append(cache[Req{}], nonesSerie)
			}

			nonesSerie.Datapoints = nones
			outSeries = append(outSeries, nonesSerie)
			continue
		}

		for _, serie1 := range inByKey[key] {
			// No total series for a corresponding input series
			if _, ok := totalSerieByKey[key]; !ok {
				nonesSerie := serie1
				nonesSerie.QueryPatt = fmt.Sprintf("asPercent(%s,MISSING)", serie1.QueryPatt)
				nonesSerie.Target = fmt.Sprintf("asPercent(%s,MISSING)", serie1.Target)
				nonesSerie.Tags = map[string]string{"name": nonesSerie.Target}
				nonesSerie.Meta = serie1.Meta.Copy()

				if nones == nil {
					nones = pointSlicePool.Get().([]schema.Point)
					for _, p := range serie1.Datapoints {
						p.Val = math.NaN()
						nones = append(nones, p)
					}
					cache[Req{}] = append(cache[Req{}], nonesSerie)
				}

				nonesSerie.Datapoints = nones
				outSeries = append(outSeries, nonesSerie)
			} else {
				// key found in both inByKey and totalSerieByKey
				serie1 = serie1.Copy(pointSlicePool.Get().([]schema.Point))
				serie2 := totalSerieByKey[key]
				serie1.QueryPatt = fmt.Sprintf("asPercent(%s,%s)", serie1.QueryPatt, serie2.QueryPatt)
				serie1.Target = fmt.Sprintf("asPercent(%s,%s)", serie1.Target, serie2.Target)
				serie1.Tags = map[string]string{"name": serie1.Target}
				for i := range serie1.Datapoints {
					serie1.Datapoints[i].Val = computeAsPercent(serie1.Datapoints[i].Val, serie2.Datapoints[i].Val)
				}
				outSeries = append(outSeries, serie1)
				cache[Req{}] = append(cache[Req{}], serie1)
			}

		}
	}
	return outSeries, nil
}

// execWithoutNodes returns the asPercent output series for each input series.
// the total (divisor) we use for each input series is based on the totals parameter, which cane be:
// * a number
// * a single series -> used as divisor consistently
// * multiple series -> must match len(series) and matched up in pairs
// * nil -> generate total by summing the inputs
func (s *FuncAsPercent) execWithoutNodes(in, totals []models.Series, cache map[Req][]models.Series) ([]models.Series, error) {
	var outSeries []models.Series
	var totalsSerie models.Series
	if math.IsNaN(s.totalFloat) && totals == nil {
		totalsSerie = sumSeries(in, cache)
		if len(in) == 1 {
			totalsSerie.Target = fmt.Sprintf("sumSeries(%s)", totalsSerie.QueryPatt)
			totalsSerie.QueryPatt = fmt.Sprintf("sumSeries(%s)", totalsSerie.QueryPatt)
			totalsSerie.Tags = map[string]string{"name": totalsSerie.Target}
		}
	} else if totals != nil {
		if len(totals) == 1 {
			totalsSerie = totals[0]
		} else if len(totals) == len(in) {
			// Sorted to match the input series with the total series based on Target.
			// Mimics Graphite's implementation
			sort.Slice(in, func(i, j int) bool {
				return in[i].Target < in[j].Target
			})
			sort.Slice(totals, func(i, j int) bool {
				return totals[i].Target < totals[j].Target
			})
		}
	} else {
		totalsSerie.QueryPatt = fmt.Sprint(s.totalFloat)
		totalsSerie.Target = fmt.Sprint(s.totalFloat)
	}

	for i, serie := range in {
		if len(totals) == len(in) {
			totalsSerie = totals[i]
		}
		serie = serie.Copy(pointSlicePool.Get().([]schema.Point))
		serie.QueryPatt = fmt.Sprintf("asPercent(%s,%s)", serie.QueryPatt, totalsSerie.QueryPatt)
		serie.Target = fmt.Sprintf("asPercent(%s,%s)", serie.Target, totalsSerie.Target)
		serie.Tags = map[string]string{"name": serie.Target}
		for i := range serie.Datapoints {
			var totalVal float64
			if len(totalsSerie.Datapoints) > 0 {
				totalVal = totalsSerie.Datapoints[i].Val
			} else {
				totalVal = s.totalFloat
			}
			serie.Datapoints[i].Val = computeAsPercent(serie.Datapoints[i].Val, totalVal)
		}
		serie.Meta = serie.Meta.Merge(totalsSerie.Meta)
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

// groupSeriesByKey groups series by their aggkey which is derived from nodes,
// and adds all seen keys to the pre-existing keys map
func groupSeriesByKey(in []models.Series, nodes []expr, keys map[string]struct{}) map[string][]models.Series {
	inByKey := make(map[string][]models.Series)
	for _, serie := range in {
		key := aggKey(serie, nodes)
		if _, ok := inByKey[key]; !ok {
			inByKey[key] = []models.Series{serie}
			keys[key] = struct{}{}
		} else {
			inByKey[key] = append(inByKey[key], serie)
		}
	}
	return inByKey
}

// getTotalSeries constructs a map with one total serie by key.
// if there is a value for the key in "inByKey", we sum the entries in totalSeriesByKey under that key,
// otherwise we do an optimization: we know that the datapoints for that key won't actually be used,
// in that case we only need to return a series that has the proper fields set like QueryPattern etc.
// note: inByKey is only used for its keys, the values (series slices) are not used.
func getTotalSeries(totalSeriesByKey, inByKey map[string][]models.Series, cache map[Req][]models.Series) map[string]models.Series {
	totalSerieByKey := make(map[string]models.Series, len(totalSeriesByKey))
	for key := range totalSeriesByKey {
		if _, ok := inByKey[key]; ok {
			totalSerieByKey[key] = sumSeries(totalSeriesByKey[key], cache)
		} else {
			totalSerieByKey[key] = totalSeriesByKey[key][0]
		}
	}
	return totalSerieByKey
}

// sumSeries returns a copy-on-write series that is the sum of the inputs
func sumSeries(in []models.Series, cache map[Req][]models.Series) models.Series {
	if len(in) == 1 {
		return in[0]
	}
	out := pointSlicePool.Get().([]schema.Point)
	crossSeriesSum(in, &out)
	var queryPatts []string
	var meta models.SeriesMeta

Loop:
	for _, v := range in {
		meta = meta.Merge(v.Meta)
		// avoid duplicates
		for _, qp := range queryPatts {
			if qp == v.QueryPatt {
				continue Loop
			}
		}
		queryPatts = append(queryPatts, v.QueryPatt)
	}
	name := fmt.Sprintf("sumSeries(%s)", strings.Join(queryPatts, ","))
	cons, queryCons := summarizeCons(in)
	sum := models.Series{
		Target:       name,
		QueryPatt:    name,
		Datapoints:   out,
		Interval:     in[0].Interval,
		Consolidator: cons,
		QueryCons:    queryCons,
		QueryFrom:    in[0].QueryFrom,
		QueryTo:      in[0].QueryTo,
		Tags:         map[string]string{"name": name},
		Meta:         meta,
	}
	cache[Req{}] = append(cache[Req{}], sum)
	return sum
}
