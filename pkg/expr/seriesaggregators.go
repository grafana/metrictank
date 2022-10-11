package expr

// aggregation functions for series of data
import (
	"math"
	"sort"

	"github.com/grafana/metrictank/api/models"
	"github.com/grafana/metrictank/schema"
)

type seriesAggregator struct {
	function crossSeriesAggFunc
	name     string
}

type crossSeriesAggFunc func(in []models.Series, out *[]schema.Point)

func getCrossSeriesAggFunc(c string) crossSeriesAggFunc {
	switch c {
	case "average", "avg":
		return crossSeriesAvg
	case "avg_zero":
		return crossSeriesAvgZero
	case "min":
		return crossSeriesMin
	case "max":
		return crossSeriesMax
	case "sum", "total":
		return crossSeriesSum
	case "multiply":
		return crossSeriesMultiply
	case "median":
		return crossSeriesMedian
	case "diff":
		return crossSeriesDiff
	case "stddev":
		return crossSeriesStddev
	case "rangeOf", "range":
		return crossSeriesRange
	case "last", "current":
		return crossSeriesLast
	case "count":
		return crossSeriesCount
	}
	return nil
}

// crossSeriesAvg computes the average across all the series
// using the number of non-null datapoints
func crossSeriesAvg(in []models.Series, out *[]schema.Point) {
	counts := make([]int, len(in[0].Datapoints))

	for i := 0; i < len(in[0].Datapoints); i++ {
		*out = append(*out, in[0].Datapoints[i])
		if !math.IsNaN(in[0].Datapoints[i].Val) {
			counts[i]++
		}
	}

	for i := 1; i < len(in); i++ {
		dps := in[i].Datapoints
		for j := 0; j < len(in[i].Datapoints); j++ {
			p := dps[j].Val
			if !math.IsNaN(p) {
				if math.IsNaN((*out)[j].Val) {
					(*out)[j].Val = p
				} else {
					(*out)[j].Val += p
				}
				counts[j]++
			}
		}
	}

	for i := range *out {
		if !math.IsNaN((*out)[i].Val) {
			(*out)[i].Val /= float64(counts[i])
		}
	}
}

// crossSeriesAvgZero computes the average across all the series
// treating nulls as valid points with value 0.
// So, if the original datapoints were [1, 1, 1, 1, null],
// crossSeriesAvg would compute the new datapoint as 4/4=1 while
// crossSeriesAvgZero would compute it as 4/5=0.8
func crossSeriesAvgZero(in []models.Series, out *[]schema.Point) {
	crossSeriesSum(in, out)
	for i := range *out {
		if !math.IsNaN((*out)[i].Val) {
			(*out)[i].Val /= float64(len(in))
		} else {
			(*out)[i].Val = 0
		}
	}
}

func crossSeriesMin(in []models.Series, out *[]schema.Point) {
	for i := 0; i < len(in[0].Datapoints); i++ {
		*out = append(*out, in[0].Datapoints[i])
	}

	for i := 1; i < len(in); i++ {
		dps := in[i].Datapoints
		for j := 0; j < len(in[i].Datapoints); j++ {
			p := dps[j].Val
			if !math.IsNaN(p) {
				v := (*out)[j].Val
				if math.IsNaN(v) || v > p {
					(*out)[j].Val = p
				}
			}
		}
	}
}

func crossSeriesMax(in []models.Series, out *[]schema.Point) {
	for i := 0; i < len(in[0].Datapoints); i++ {
		*out = append(*out, in[0].Datapoints[i])
	}

	for i := 1; i < len(in); i++ {
		dps := in[i].Datapoints
		for j := 0; j < len(in[i].Datapoints); j++ {
			p := dps[j].Val
			if !math.IsNaN(p) {
				v := (*out)[j].Val
				if math.IsNaN(v) || v < p {
					(*out)[j].Val = p
				}
			}
		}
	}
}

func crossSeriesSum(in []models.Series, out *[]schema.Point) {
	for i := 0; i < len(in[0].Datapoints); i++ {
		*out = append(*out, in[0].Datapoints[i])
	}

	for i := 1; i < len(in); i++ {
		dps := in[i].Datapoints
		for j := 0; j < len(in[i].Datapoints); j++ {
			p := dps[j].Val
			if !math.IsNaN(p) {
				if math.IsNaN((*out)[j].Val) {
					(*out)[j].Val = p
				} else {
					(*out)[j].Val += p
				}
			}
		}
	}
}

func crossSeriesMultiply(in []models.Series, out *[]schema.Point) {
	for i := 0; i < len(in[0].Datapoints); i++ {
		*out = append(*out, in[0].Datapoints[i])
	}

	for i := 1; i < len(in); i++ {
		dps := in[i].Datapoints
		for j := 0; j < len(in[i].Datapoints); j++ {
			(*out)[j].Val *= dps[j].Val
		}
	}
}

func crossSeriesMedian(in []models.Series, out *[]schema.Point) {
	vals := make([]float64, 0, len(in))
	for i := 0; i < len(in[0].Datapoints); i++ {
		vals = vals[:0]
		for j := 0; j < len(in); j++ {
			p := in[j].Datapoints[i].Val
			if !math.IsNaN(p) {
				vals = append(vals, p)
			}
		}
		point := schema.Point{
			Ts: in[0].Datapoints[i].Ts,
		}
		if len(vals) == 0 {
			point.Val = math.NaN()
		} else {
			sort.Float64s(vals)
			mid := len(vals) / 2
			if len(vals)%2 == 0 {
				point.Val = (vals[mid-1] + vals[mid]) / 2
			} else {
				point.Val = vals[mid]
			}
		}

		*out = append(*out, point)
	}
}

func crossSeriesDiff(in []models.Series, out *[]schema.Point) {
	for i := 0; i < len(in[0].Datapoints); i++ {
		*out = append(*out, in[0].Datapoints[i])
	}

	for i := 1; i < len(in); i++ {
		for j := 0; j < len(in[i].Datapoints); j++ {
			p := in[i].Datapoints[j].Val
			if !math.IsNaN(p) {
				if math.IsNaN((*out)[j].Val) {
					(*out)[j].Val = p
				} else {
					(*out)[j].Val -= p
				}
			}

		}
	}
}

func crossSeriesStddev(in []models.Series, out *[]schema.Point) {
	crossSeriesAvg(in, out)

	for i := 0; i < len(in[0].Datapoints); i++ {
		if !math.IsNaN((*out)[i].Val) {
			num := float64(0)
			totalDeviationSquared := float64(0)
			for j := 0; j < len(in); j++ {
				p := in[j].Datapoints[i].Val
				if !math.IsNaN(p) {
					num++
					deviation := p - (*out)[i].Val
					totalDeviationSquared += deviation * deviation
				}
			}

			(*out)[i].Val = math.Sqrt(totalDeviationSquared / num)
		}
	}
}

func crossSeriesRange(in []models.Series, out *[]schema.Point) {
	mins := make([]schema.Point, 0, len(in[0].Datapoints))

	crossSeriesMax(in, out)
	crossSeriesMin(in, &mins)

	for i := 0; i < len(in[0].Datapoints); i++ {
		(*out)[i].Val -= mins[i].Val
	}
}

func crossSeriesLast(in []models.Series, out *[]schema.Point) {
	for i := 0; i < len(in[len(in)-1].Datapoints); i++ {
		dp := in[len(in)-1].Datapoints[i]
		for j := len(in) - 2; j >= 0 && math.IsNaN(dp.Val); j-- {
			dp = in[j].Datapoints[i]
		}
		*out = append(*out, dp)
	}
}

func crossSeriesCount(in []models.Series, out *[]schema.Point) {

	for i := 0; i < len(in[0].Datapoints); i++ {
		point := schema.Point{
			Ts: in[0].Datapoints[i].Ts,
		}
		point.Val = 0
		for j := 0; j < len(in); j++ {
			if !math.IsNaN(in[j].Datapoints[i].Val) {
				point.Val++
			}
		}
		if point.Val == 0 {
			point.Val = math.NaN()
		}
		*out = append(*out, point)
	}
}
