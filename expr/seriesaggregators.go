package expr

// aggregation functions for series of data
import (
	"math"
	"sort"

	"github.com/grafana/metrictank/api/models"
	"gopkg.in/raintank/schema.v1"
)

type seriesAggregator struct {
	function crossSeriesAggFunc
	name     string
}

type crossSeriesAggFunc func(in []models.Series, out *[]schema.Point)

func getCrossSeriesAggFunc(c string) crossSeriesAggFunc {
	switch c {
	case "avg", "average":
		return crossSeriesAvg
	case "min":
		return crossSeriesMin
	case "max":
		return crossSeriesMax
	case "sum":
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
	}
	return nil
}

func crossSeriesAvg(in []models.Series, out *[]schema.Point) {
	for i := 0; i < len(in[0].Datapoints); i++ {
		num := 0
		sum := float64(0)
		for j := 0; j < len(in); j++ {
			p := in[j].Datapoints[i].Val
			if !math.IsNaN(p) {
				num++
				sum += p
			}
		}
		point := schema.Point{
			Ts: in[0].Datapoints[i].Ts,
		}
		if num == 0 {
			point.Val = math.NaN()
		} else {
			point.Val = sum / float64(num)
		}

		*out = append(*out, point)
	}
}

func crossSeriesMin(in []models.Series, out *[]schema.Point) {
	for i := 0; i < len(in[0].Datapoints); i++ {
		nan := true
		min := math.Inf(1)
		for j := 0; j < len(in); j++ {
			p := in[j].Datapoints[i].Val
			if !math.IsNaN(p) && p < min {
				nan = false
				min = p
			}
		}

		point := schema.Point{
			Ts: in[0].Datapoints[i].Ts,
		}
		if nan {
			point.Val = math.NaN()
		} else {
			point.Val = min
		}

		*out = append(*out, point)
	}
}

func crossSeriesMax(in []models.Series, out *[]schema.Point) {
	for i := 0; i < len(in[0].Datapoints); i++ {
		nan := true
		max := math.Inf(-1)
		for j := 0; j < len(in); j++ {
			p := in[j].Datapoints[i].Val
			if !math.IsNaN(p) && p > max {
				nan = false
				max = p
			}
		}

		point := schema.Point{
			Ts: in[0].Datapoints[i].Ts,
		}
		if nan {
			point.Val = math.NaN()
		} else {
			point.Val = max
		}

		*out = append(*out, point)
	}
}

func crossSeriesSum(in []models.Series, out *[]schema.Point) {
	for i := 0; i < len(in[0].Datapoints); i++ {
		nan := true
		sum := float64(0)
		for j := 0; j < len(in); j++ {
			p := in[j].Datapoints[i].Val
			if !math.IsNaN(p) {
				nan = false
				sum += p
			}
		}
		point := schema.Point{
			Ts: in[0].Datapoints[i].Ts,
		}
		if nan {
			point.Val = math.NaN()
		} else {
			point.Val = sum
		}

		*out = append(*out, point)
	}
}

func crossSeriesMultiply(in []models.Series, out *[]schema.Point) {
	for i := 0; i < len(in[0].Datapoints); i++ {
		product := float64(1)
		for j := 0; j < len(in); j++ {
			p := in[j].Datapoints[i].Val
			if math.IsNaN(p) {
				// NaN * anything equals NaN()
				product = math.NaN()
				break
			}
			product *= p
		}
		point := schema.Point{
			Ts:  in[0].Datapoints[i].Ts,
			Val: product,
		}

		*out = append(*out, point)
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
		val := math.NaN()
		for j := 0; j < len(in); j++ {
			p := in[j].Datapoints[i].Val
			if !math.IsNaN(p) {
				if math.IsNaN(val) {
					val = p
				} else {
					val -= p
				}
			}
		}
		point := schema.Point{
			Ts:  in[0].Datapoints[i].Ts,
			Val: val,
		}

		*out = append(*out, point)
	}
}

func crossSeriesStddev(in []models.Series, out *[]schema.Point) {
	averages := make([]schema.Point, 0, len(in[0].Datapoints))
	crossSeriesAvg(in, &averages)

	for i := 0; i < len(in[0].Datapoints); i++ {
		point := schema.Point{
			Ts:  in[0].Datapoints[i].Ts,
			Val: math.NaN(),
		}

		if !math.IsNaN(averages[i].Val) {
			num := float64(0)
			totalDeviationSquared := float64(0)
			for j := 0; j < len(in); j++ {
				p := in[j].Datapoints[i].Val
				if !math.IsNaN(p) {
					num++
					deviation := p - averages[i].Val
					totalDeviationSquared += deviation * deviation
				}
			}

			point.Val = math.Sqrt(totalDeviationSquared / num)
		}

		*out = append(*out, point)
	}
}

func crossSeriesRange(in []models.Series, out *[]schema.Point) {
	maxes := make([]schema.Point, 0, len(in[0].Datapoints))
	mins := make([]schema.Point, 0, len(in[0].Datapoints))

	crossSeriesMax(in, &maxes)
	crossSeriesMin(in, &mins)

	for i := 0; i < len(in[0].Datapoints); i++ {
		point := schema.Point{
			Ts:  in[0].Datapoints[i].Ts,
			Val: math.NaN(),
		}

		if !math.IsNaN(maxes[i].Val) {
			point.Val = maxes[i].Val - mins[i].Val
		}

		*out = append(*out, point)
	}
}
